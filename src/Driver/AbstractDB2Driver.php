<?php

namespace DoctrineDbalIbmi\Driver;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use DoctrineDbalIbmi\Platform\DB2IBMiPlatform;
use DoctrineDbalIbmi\Schema\DB2IBMiSchemaManager;

abstract class AbstractDB2Driver implements Driver
{
    const SYSTEM_IBMI = 'AIX';
    const SYSTEM_IBMI_OS400 = 'OS400';

    /**
     * @return true
     */
    public static function isIbmi()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabase(Connection $conn)
    {
        $params = $conn->getParams();

        if (DB2Driver::class === $params['driverClass'] && isset($params['driverOptions']) && is_array($params['driverOptions'])
            && isset($params['driverOptions']['i5_lib'])) {
            // In iSeries systems, with SQL naming, the default database name is specified in ['driverOptions' => ['i5_lib' = ?]]
            $database = $params['driverOptions']['i5_lib'];
        } else {
            $dsnParams = DataSourceName::fromConnectionParameters($params)
                ->getConnectionParameters();
            $database = $dsnParams['DATABASE'];
        }

        assert(is_string($database));

        return $database;
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
    public function getSchemaManager(Connection $conn)
    {
        return new DB2IBMiSchemaManager($conn);
    }
}
