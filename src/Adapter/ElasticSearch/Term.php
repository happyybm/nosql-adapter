<?php
namespace Nosql\Adapter\ElasticSearch;
/**
 * 查询条件
 * @author roy
 *
 */
abstract class Term
{

    /**
     * 转化为数组
     */
    abstract function toArray();
}