<?php

namespace App\Fogger\Subset;

use App\Fogger\Recipe\Table;
use Doctrine\DBAL\Query\QueryBuilder;

class WhereSQLSubset extends AbstractSubset
{
    const SUBSET_STRATEGY_NAME = 'where_sql';
    const CONFIGURED_SQL = 'sql';

    /**
     * @param array $options
     * @throws Exception\RequiredOptionMissingException
     */
    private function ensureValidOptions(array $options)
    {
        $this->ensureOptionIsSet($options, self::CONFIGURED_SQL);
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

        return $queryBuilder->where(sprintf('(%s)', $options['sql']));
    }

    public function getSubsetStrategyName(): string
    {
        return self::SUBSET_STRATEGY_NAME;
    }
}
