<?php

namespace Nosql;

use Nosql\DbException;
use Nosql\Adapter\AbstractDb;
use Nosql\Adapter\DynamoDb;
use Nosql\Adapter\ElasticSearchDb;

/**
 * adapter的工厂类。用于生成adapter
 */
class Db {
	
	/**
	 * 生成一个适配器对象
	 * 
	 * @param string $adapter
	 *        	适配器名称
	 * @param array  $config
	 *        	链接配置信息
	 * @throws DbException
	 * @return Ambigous 
	 */
	public static function factory($adapter, $config = array()) {
		/*
		 * Verify that adapter parameters are in an array.
		 */
		if (! is_array ( $config )) {
			throw new DbException ( 'Adapter parameters must be  an array ' );
		}
		
		/*
		 * Verify that an adapter name has been specified.
		 */
		if (! is_string ( $adapter ) || empty ( $adapter )) {
			throw new DbException ( 'Adapter name must be specified in a string' );
		}
		/*
		 * Load the adapter class. This throws an exception if the specified class cannot be loaded.
		 */
		$adapterName = ucfirst ( $adapter );
		if (! class_exists ( $adapterName )) {
		    throw new DbException ( 'Adapter '.$adapterName.' class not fund' );
		}
		
		/*
		 * Create an instance of the adapter class. Pass the config to the adapter class constructor.
		 */
		$dbAdapter = new $adapterName ( $config );
		/*
		 * Verify that the object created is a descendent of the abstract adapter type.
		 */
		if (! $dbAdapter instanceof AbstractDb) {
			throw new DbException ( "Adapter class '$adapterName' does not extend AbstractDb");
		}
		return $dbAdapter;
	}

}
