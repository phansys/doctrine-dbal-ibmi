<?php

/*
 * This file is part of the doctrine-dbal-ibmi package.
 * Copyright (c) 2016 Alan Seiden Consulting LLC, James Titcumb
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace DoctrineDbalIbmi\Driver;

use Doctrine\DBAL\Driver\IBMDB2\Exception\ConnectionFailed;
use DoctrineDbalIbmi\Driver\AbstractDB2Driver;
use DoctrineDbalIbmi\Driver\DataSourceName;

class DB2Driver extends AbstractDB2Driver
{
    /**
     * {@inheritdoc}
     */
    public function connect(array $params)
    {
        $params['protocol'] = $params['protocol'] ?? 'TCPIP';
        $params['driver'] = '{IBM DB2 ODBC DRIVER}';
        $params['dbname'] = DataSourceName::fromConnectionParameters($params)->toString();

        unset($params['driver'], $params['user'], $params['password'], $params['host'], $params['port'], $params['protocol']);

        $driverOptions = $params['driverOptions'] ?? [];

        if ($params['persistent'] ?? false) {
            $connection = \db2_pconnect($params['dbname'], null, null, $driverOptions);
        } else {
            $connection = \db2_connect($params['dbname'], null, null, $driverOptions);
        }

        if ($connection === false) {
            throw ConnectionFailed::new();
        }

        return new DB2IBMiConnection($connection, $driverOptions);
    }

    /**
     * {@inheritdoc}
     */
    public function getName()
    {
        return 'ibm_db2_i';
    }
}
