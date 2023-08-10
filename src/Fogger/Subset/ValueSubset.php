<?php

namespace App\Fogger\Subset;

use App\Fogger\Recipe\Table;
use Doctrine\DBAL\Query\QueryBuilder;

class ValueSubset extends AbstractSubset
{
    const SUBSET_STRATEGY_NAME = 'value';
    const CONFIGURED_VALUES = 'values';
    const CONFIGURED_COLUMN_NAME = 'column';

    /**
     * @param array $options
     * @throws Exception\RequiredOptionMissingException
     */
    private function ensureValidOptions(array $options)
    {
        $this->ensureOptionIsSet($options, self::CONFIGURED_COLUMN_NAME);
        $this->ensureOptionIsSet($options, self::CONFIGURED_VALUES);
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

        return $queryBuilder
            ->where(sprintf('`%s` in (:values)', $options['column']))
            ->setParameter('values', $options['values'], \Doctrine\DBAL\Connection::PARAM_INT_ARRAY);
    }

    public function getSubsetStrategyName(): string
    {
        return self::SUBSET_STRATEGY_NAME;
    }
}
