<?php

namespace App\Fogger\Recipe;

use App\Config\ConfigLoader;
use App\Fogger\Data\ChunkMessage;
use Doctrine\DBAL\Connection;

class RecipeFactory
{
    private $configLoader;

    private $sourceSchema;

    private $recipeTableFactory;

    private $maskReplicator;

    private static $recipe = null;

    public function __construct(
        ConfigLoader $configLoader,
        Connection $connection,
        RecipeTableFactory $recipeTableFactory,
        MaskReplicator $maskReplicator
    ) {
        $this->configLoader = $configLoader;
        $this->sourceSchema = $connection->getSchemaManager();
        $this->recipeTableFactory = $recipeTableFactory;
        $this->maskReplicator = $maskReplicator;
    }

    /**
     * @param string $configFilename
     * @param int $chunkSize
     * @return Recipe
     * @throws \Doctrine\DBAL\DBALException
     */
    public function createRecipe(string $configFilename, int $chunkSize = ChunkMessage::DEFAULT_CHUNK_SIZE)
    {
        if (self::$recipe != null) {
            return self::$recipe;
        }

        $config = $this->configLoader->load($configFilename);
        $recipe = new Recipe($config->getExcludes());
        self::$recipe = $recipe;

        foreach ($this->sourceSchema->listTables() as $dbalTable) {
            $tableName = $dbalTable->getName();
            if (!in_array($tableName, $config->getExcludes())) {
                $recipe->addTable(
                    $tableName,
                    $this->recipeTableFactory->createRecipeTable($dbalTable, $chunkSize, $config->getTable($tableName))
                );
            }
        }
        $this->maskReplicator->replicateMasksToRelatedColumns($recipe);

        return $recipe;
    }

    public static function getRecipe(): Recipe
    {
        return self::$recipe;
    }
}
