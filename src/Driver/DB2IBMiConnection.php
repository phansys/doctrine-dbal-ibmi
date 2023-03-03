<?php

/*
 * This file is part of the doctrine-dbal-ibmi package.
 * Copyright (c) 2016 Alan Seiden Consulting LLC, James Titcumb
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace DoctrineDbalIbmi\Driver;

use Doctrine\DBAL\Driver\IBMDB2\Exception\ConnectionError;
use Doctrine\DBAL\Driver\IBMDB2\Exception\PrepareFailed;
use Doctrine\DBAL\Driver\IBMDB2\Exception\StatementError;
use Doctrine\DBAL\Driver\IBMDB2\Statement;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use Doctrine\DBAL\Driver\Statement as DriverStatement;
use Doctrine\DBAL\ParameterType;

/**
 * IBMi Db2 Connection.
 * More documentation about iSeries schema at https://www-01.ibm.com/support/knowledgecenter/ssw_ibm_i_72/db2/rbafzcatsqlcolumns.htm
 *
 * @author Cassiano Vailati <c.vailati@esconsulting.it>
 * @author James Titcumb <james@asgrim.com>
 */
class DB2IBMiConnection implements ServerInfoAwareConnection
{
    protected $driverOptions = [];

    /** @var resource */
    private $connection;

    /**
     * @internal The connection can be only instantiated by its driver.
     *
     * @param resource $connection
     */
    public function __construct($connection, ?array $driverOptions = [])
    {
        $this->connection = $connection;
        $this->driverOptions = $driverOptions;
    }

    /**
     * {@inheritdoc}
     */
    public function getServerVersion()
    {
        $serverInfo = \db2_server_info($this->connection);
        assert($serverInfo instanceof \stdClass);

        return $serverInfo->DBMS_VER;
    }

    public function prepare(string $sql): DriverStatement
    {
        $stmt = @\db2_prepare($this->connection, $sql);

        if ($stmt === false) {
            throw PrepareFailed::new(error_get_last());
        }

        return new Statement($stmt);
    }

    public function query(string $sql): ResultInterface
    {
        return $this->prepare($sql)->execute();
    }

    /**
     * {@inheritdoc}
     */
    public function quote($value, $type = ParameterType::STRING)
    {
        $value = \db2_escape_string($value);

        if ($type === ParameterType::INTEGER) {
            return $value;
        }

        return "'" . $value . "'";
    }

    public function exec(string $sql): int
    {
        $stmt = @\db2_exec($this->connection, $sql);

        if ($stmt === false) {
            throw StatementError::new();
        }

        return \db2_num_rows($stmt);
    }

    // /**
    //  * {@inheritdoc}
    //  */
    // public function lastInsertId($name = null)
    // {
    //     if ($name !== null) {
    //         Deprecation::triggerIfCalledFromOutside(
    //             'doctrine/dbal',
    //             'https://github.com/doctrine/dbal/issues/4687',
    //             'The usage of Connection::lastInsertId() with a sequence name is deprecated.',
    //         );
    //     }

    //     return db2_last_insert_id($this->connection) ?? false;
    // }

    public function beginTransaction(): bool
    {
        return \db2_autocommit($this->connection, \DB2_AUTOCOMMIT_OFF);
    }

    public function commit(): bool
    {
        if (! \db2_commit($this->connection)) {
            throw ConnectionError::new($this->connection);
        }

        return \db2_autocommit($this->connection, \DB2_AUTOCOMMIT_ON);
    }

    public function rollBack(): bool
    {
        if (! \db2_rollback($this->connection)) {
            throw ConnectionError::new($this->connection);
        }

        return \db2_autocommit($this->connection, \DB2_AUTOCOMMIT_ON);
    }

    /** @return resource */
    public function getNativeConnection()
    {
        return $this->connection;
    }

    /**
     * {@inheritdoc}
     */
    public function lastInsertId($name = null)
    {
        $sql = 'SELECT IDENTITY_VAL_LOCAL() AS VAL FROM QSYS2'.$this->getSchemaSeparatorSymbol().'QSQPTABL';
        $stmt = $this->prepare($sql);
        $stmt->execute();

        $res = $stmt->fetch();

        assert(is_array($res));

        return $res['VAL'];
    }

    /**
     * Returns the appropriate schema separation symbol for i5 systems.
     * Other systems can hardcode '.' but i5 may need '.' or  '/' depending on the naming mode.
     *
     * @return string
     */
    public function getSchemaSeparatorSymbol()
    {
        // if "i5 naming" is on, use '/' to separate schema and table. Otherwise use '.'
        if (array_key_exists('i5_naming', $this->driverOptions) && $this->driverOptions['i5_naming']) {
            // "i5 naming" mode requires a slash
            return '/';
        }
        // SQL naming requires a dot
        return '.';
    }

    /**
     * Retrieves ibm_db2 native resource handle.
     *
     * Could be used if part of your application is not using DBAL.
     *
     * @return resource
     */
    public function getWrappedResourceHandle()
    {
        return $this->getNativeConnection();
    }
}
