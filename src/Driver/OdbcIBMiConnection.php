<?php

/*
 * This file is part of the doctrine-dbal-ibmi package.
 * Copyright (c) 2016 Alan Seiden Consulting LLC, James Titcumb
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace DoctrineDbalIbmi\Driver;

use Doctrine\DBAL\Driver\PDO\Exception;
use Doctrine\DBAL\Driver\PDO\PDOException as DriverPDOException;
use Doctrine\DBAL\Driver\PDO\Statement;
use Doctrine\DBAL\Driver\PDO\Result;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use Doctrine\DBAL\Driver\Statement as StatementInterface;

/**
 * IBMi Db2 Connection.
 * More documentation about iSeries schema at https://www-01.ibm.com/support/knowledgecenter/ssw_ibm_i_72/db2/rbafzcatsqlcolumns.htm
 *
 * @author Cassiano Vailati <c.vailati@esconsulting.it>
 * @author James Titcumb <james@asgrim.com>
 */
class OdbcIBMiConnection implements ServerInfoAwareConnection
{
    private \PDO $connection;

    /** @internal The connection can be only instantiated by its driver. */
    public function __construct(\PDO $connection, ?array $driverOptions = [])
    {
        $connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $this->driverOptions = $driverOptions;
        $this->driverOptions[\PDO::ATTR_PERSISTENT] = false;
        if (isset($driverOptions['persistent'])) {
            $this->driverOptions[\PDO::ATTR_PERSISTENT] = $driverOptions['persistent'];
        }

        $this->connection = $connection;
    }

    public function exec(string $sql): int
    {
        try {
            $result = $this->connection->exec($sql);

            assert($result !== false);

            return $result;
        } catch (\PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getServerVersion()
    {
        return $this->connection->getAttribute(\PDO::ATTR_SERVER_VERSION);
    }

    /**
     * {@inheritDoc}
     *
     * @return Statement
     */
    public function prepare(string $sql): StatementInterface
    {
        try {
            $stmt = $this->connection->prepare($sql);
            assert($stmt instanceof \PDOStatement);

            return new Statement($stmt);
        } catch (\PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    public function query(string $sql): ResultInterface
    {
        try {
            $stmt = $this->connection->query($sql);
            assert($stmt instanceof \PDOStatement);

            return new Result($stmt);
        } catch (\PDOException $exception) {
            throw Exception::new($exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function quote($value, $type = ParameterType::STRING)
    {
        return $this->connection->quote($value, ParameterTypeMap::convertParamType($type));
    }

    // /**
    //  * {@inheritdoc}
    //  */
    // public function lastInsertId($name = null)
    // {
    //     try {
    //         if ($name === null) {
    //             return $this->connection->lastInsertId();
    //         }

    //         Deprecation::triggerIfCalledFromOutside(
    //             'doctrine/dbal',
    //             'https://github.com/doctrine/dbal/issues/4687',
    //             'The usage of Connection::lastInsertId() with a sequence name is deprecated.',
    //         );

    //         return $this->connection->lastInsertId($name);
    //     } catch (PDOException $exception) {
    //         throw Exception::new($exception);
    //     }
    // }

    public function beginTransaction(): bool
    {
        try {
            return $this->connection->beginTransaction();
        } catch (PDOException $exception) {
            throw DriverPDOException::new($exception);
        }
    }

    public function commit(): bool
    {
        try {
            return $this->connection->commit();
        } catch (PDOException $exception) {
            throw DriverPDOException::new($exception);
        }
    }

    public function rollBack(): bool
    {
        try {
            return $this->connection->rollBack();
        } catch (PDOException $exception) {
            throw DriverPDOException::new($exception);
        }
    }

    public function getNativeConnection(): \PDO
    {
        return $this->connection;
    }

    /** @deprecated Call {@see getNativeConnection()} instead. */
    public function getWrappedConnection(): \PDO
    {
        Deprecation::triggerIfCalledFromOutside(
            'doctrine/dbal',
            'https://github.com/doctrine/dbal/pull/5037',
            '%s is deprecated, call getNativeConnection() instead.',
            __METHOD__,
        );

        return $this->getNativeConnection();
    }

    protected $driverOptions = array();

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
        $connProperty = new \ReflectionProperty(DB2Connection::class, '_conn');
        $connProperty->setAccessible(true);
        $handle = $connProperty->getValue($this);

        assert(is_resource($handle));

        return $handle;
    }

    /**
     * @return true
     */
    public function requiresQueryForServerVersion()
    {
        return true;
    }
}
