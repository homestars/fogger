<?php

namespace App\Fogger\Subset;

use App\Fogger\Recipe\Table;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Connection;
use App\Fogger\Recipe\RecipeFactory;

class ForeignKeyLookupSubset extends AbstractSubset
{
    const SUBSET_STRATEGY_NAME = 'foreign_key_lookup';
    const CONFIGURED_PARENT_TABLE_NAME = 'parent_table_name';
    const CONFIGURED_OPTIONAL_PARENT_COLUMN_NAME = 'optional_parent_column_name';
    const CONFIGURED_CHILD_COLUMN = 'child_table_column_name';

    /**
     * @param array $options
     * @throws Exception\RequiredOptionMissingException
     */
    private function ensureValidOptions(array $options)
    {
        $this->ensureOptionIsSet($options, self::CONFIGURED_PARENT_TABLE_NAME);
        $this->ensureOptionIsSet($options, self::CONFIGURED_CHILD_COLUMN);
    }

    /**
     * @param QueryBuilder $queryBuilder
     * @param Table $table
     * @return QueryBuilder
     * @throws Exception\RequiredOptionMissingException
     */
    public function subsetQuery(QueryBuilder $queryBuilder, Table $table): QueryBuilder
    {
        $this->ensureValidOptions($options = $table->getSubset()->getOptions());

        $subquery = $this->getInSubQuery(
            $queryBuilder->getConnection(),
            $options[self::CONFIGURED_PARENT_TABLE_NAME],
            $options[self::CONFIGURED_CHILD_COLUMN],
            $options[self::CONFIGURED_OPTIONAL_PARENT_COLUMN_NAME]
        );
        if ($subquery) {
            $queryBuilder = $queryBuilder->where($subquery);
        }

        return $queryBuilder;
    }

    public function getSubsetStrategyName(): string
    {
        return self::SUBSET_STRATEGY_NAME;
    }

    private function getInSubQuery(Connection $source, String $tableName, String $columnName, ?String $optionalParentColumnName): ?string
    {
        $table = $this->getParentTable($tableName);
        $column = $optionalParentColumnName ?? $table->getSortBy();
        $results = $this->getResults($source, $table, $column);

        if (empty($results)) {
            print_r("No results found for table $tableName");
            return null;
        } else {
            $ids = call_user_func_array('array_merge', array_map("array_values", $results));
            $idsIn = implode(",", $ids);

            return "$columnName IN ($idsIn)";
        }
    }

    private function getAllKeysQuery(Connection $source, Table $table, String $columnName): QueryBuilder
    {
        $query = $source->createQueryBuilder();
        $query
            ->select($source->quoteIdentifier($columnName))
            ->from($source->quoteIdentifier($table->getName()));

        $subset = new ValueSubset();
        return $subset->subsetQuery($query, $table);
    }

    private function getParentTable(String $tableName): Table
    {
        $recipe = RecipeFactory::getRecipe();
        $table = $recipe->getTable($tableName);
        if (!$table || $table->getSubset()->getName() !== ValueSubset::SUBSET_STRATEGY_NAME) {
            throw new \Exception("Table $tableName not found or not using ValueSubset strategy");
        }
        return $table;
    }

    private function getResults(Connection $source, Table $table, String $columnName): array
    {
        $query = $this->getAllKeysQuery($source, $table, $columnName);
        return $query->execute()->fetchAll();
    }
}
