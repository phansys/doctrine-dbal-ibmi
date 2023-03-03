<?php

/*
 * This file is part of the doctrine-dbal-ibmi package.
 * Copyright (c) 2016 Alan Seiden Consulting LLC, James Titcumb
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace DoctrineDbalIbmi\Driver;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDO\Exception;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\VersionAwarePlatformDriver;
use DoctrineDbalIbmi\Platform\DB2IBMiPlatform;
use DoctrineDbalIbmi\Schema\DB2IBMiSchemaManager;

class OdbcDriver extends AbstractDB2Driver implements VersionAwarePlatformDriver
{
    /**
     * {@inheritdoc}
     */
    public function connect(array $params)
    {
        // @todo: Remove the following conditional block in the next major version.
        if (isset($params['username'])) {
            @trigger_error(sprintf(
                'Passing parameter "username" to "%s()" is deprecated since alanseiden/doctrine-dbal-ibmi 0.1 and its support'
                .' will be removed in version 0.2. Use "user" parameter instead.',
                __METHOD__
            ), E_USER_DEPRECATED);

            $params['user'] = $params['username'];
            unset($params['username']);
        }

        assert(is_scalar($params['user']));
        assert(is_scalar($params['password']));

        $params['driver'] = '{IBM i Access ODBC Driver}';
        $params['dsn'] = 'odbc:'.DataSourceName::fromConnectionParameters($params)->toString();

        unset($params['driver'], $params['host'], $params['port'], $params['protocol']);

        if (! empty($params['persistent'])) {
            $driverOptions[\PDO::ATTR_PERSISTENT] = true;
        }

        $safeParams = $params;
        unset($safeParams['password'], $safeParams['url']);

        try {
            $pdo = new \PDO(
                $this->constructPdoDsn($safeParams),
                $params['user'] ?? '',
                $params['password'] ?? '',
                $driverOptions,
            );
        } catch (\PDOException $exception) {
            throw Exception::new($exception);
        }

        return new OdbcIBMiConnection($pdo);
    }

    /**
     * {@inheritdoc}
     */
    public function getName()
    {
        return 'pdo_odbc_ibm_db2_i';
    }

    public function createDatabasePlatformForVersion($version)
    {
        return new DB2IBMiPlatform();
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabase(Connection $conn)
    {
        $params = $conn->getParams();

        return $params['dbname'];
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabasePlatform()
    {
        return new DB2IBMiPlatform();
    }

    /**
     * {@inheritdoc}
     */
    public function getSchemaManager(Connection $conn, AbstractPlatform $platform)
    {
        return new DB2IBMiSchemaManager($conn, $platform);
    }

    /**
     * Constructs the MySQL PDO DSN.
     *
     * @param mixed[] $params
     */
    private function constructPdoDsn(array $params): string
    {
        if (isset($params['dsn']) && $params['dsn'] !== '') {
            return 'odbc:'.$params['dsn'];
        }

        $dsn = 'odbc:DRIVER={IBM i Access ODBC Driver};';

        if (isset($params['host']) && $params['host'] !== '') {
            $dsn .= 'SYSTEM=' . $params['host'] . ';';
        }

        if (isset($params['port'])) {
            $dsn .= 'PORT=' . $params['port'] . ';';
        }

        if (isset($params['dbname'])) {
            $dsn .= 'DATABASE=' . $params['dbname'] . ';';
        }

        if (isset($params['protocol'])) {
            $dsn .= 'PROTOCOL=' . $params['protocol'] . ';';
        } else {
            $dsn .= 'PROTOCOL=TCPIP;';
        }

        // if (isset($params['charset'])) {
        //     $dsn .= 'charset=' . $params['charset'] . ';';
        // }

        return $dsn;
    }
}
