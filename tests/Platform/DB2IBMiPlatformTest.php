<?php

namespace DoctrineDbalIbmi\Tests\Platform;

use DoctrineDbalIbmi\Driver\DB2Driver;
use DoctrineDbalIbmi\Tests\AbstractTestCase;

final class DB2IBMiPlatformTest extends AbstractTestCase
{
    /**
     * @return iterable<mixed, array<int, string>>
     */
    public function typeMappingProvider(): iterable
    {
        return [
            ['smallint', 'smallint'],
            ['bigint', 'bigint'],
            ['integer', 'integer'],
            ['rowid', 'integer'],
            ['time', 'time'],
            ['date', 'date'],
            ['varchar', 'string'],
            ['character', 'string'],
            ['char', 'string'],
            ['nvarchar', 'string'],
            ['nchar', 'string'],
            ['char () for bit data', 'string'],
            ['varchar () for bit data', 'string'],
            ['varg', 'string'],
            ['vargraphic', 'string'],
            ['graphic', 'string'],
            ['varbinary', 'binary'],
            ['binary', 'binary'],
            ['varbin', 'binary'],
            ['clob', 'text'],
            ['nclob', 'text'],
            ['dbclob', 'text'],
            ['blob', 'blob'],
            ['decimal', 'decimal'],
            ['numeric', 'float'],
            ['double', 'float'],
            ['real', 'float'],
            ['float', 'float'],
            ['timestamp', 'datetime'],
            ['timestmp', 'datetime'],
        ];
    }

    /**
     * @requires ibm_db2
     *
     * @return void
     *
     * @dataProvider typeMappingProvider
     */
    public function testTypeMappings(string $dbType, string $expectedMapping)
    {
        $connection = self::getConnection(DB2Driver::class);
        $platform = $connection->getDatabasePlatform();

        self::assertSame($expectedMapping, $platform->getDoctrineTypeMapping($dbType));
    }

    /**
     * @return iterable<mixed, array<int, string|array<string, int|bool>>>
     */
    public function varcharTypeDeclarationProvider(): iterable
    {
        return [
            ['VARCHAR(1024)', ['length' => 1024]],
            ['VARCHAR(255)', []],
            ['VARCHAR(255)', ['length' => 0]],
            ['CLOB(1M)', ['fixed' => true, 'length' => 1024]],
            ['CHAR(255)', ['fixed' => true]],
            ['CHAR(255)', ['fixed' => true, 'length' => 0]],
            ['CLOB(1M)', ['length' => 5000]],
        ];
    }

    /**
     * @requires ibm_db2
     *
     * @return void
     *
     * @dataProvider varcharTypeDeclarationProvider
     */
    public function testVarcharTypeDeclarationSQLSnippet(string $expectedSql, array $fieldDef)
    {
        $connection = self::getConnection(DB2Driver::class);
        $platform = $connection->getDatabasePlatform();

        self::assertSame($expectedSql, $platform->getVarcharTypeDeclarationSQL($fieldDef));
    }
}
