<?php
namespace Nosql\Select;

use Nosql\Select\Select;
use Nosql\Adapter\ElasticSearch\MatchTerm;
use Nosql\Adapter\ElasticSearch\ExistsTerm;
use Nosql\Adapter\ElasticSearch\BoolTerm;
use Nosql\Adapter\ElasticSearch\RangeTerm;
use Nosql\Adapter\ElasticSearch\InTerm;
use Nosql\Adapter\ElasticSearch\WildcardTerm;
use Nosql\Adapter\ElasticSearch\Term;

class ElasticSearchSelect extends Select
{
    const KEY_TRUE = "must";
    const KEY_NOT = "must_not";
    const KEY_SHOULD = "should";
    /**
     * 多个索引同时搜索
     *
     * @var array
     */
    private $indexs = [ ];
    private $params = [ ];

    /*
     * (non-PHPdoc)
     * @see \Duomai\Db\Select\Select::from()
     */
    public function from($indexName)
    {
        if (! in_array ( $indexName, $this->indexs )) {
            $this->indexs [] = $indexName;
        }
        return $this;
    }

    /**
     * 根据不同的条件，指定使用的查询方式：Query,Scan
     */
    protected function assemble()
    {
        // 分析查询逻辑条件
        foreach ( $this->ands as $expression ) {
            $nextCond = $this->explorCond ( $expression );
            if($nextCond){
                $this->selectConds->addNextCond ( $nextCond, self::COND_AND );
            }
        }
        foreach ( $this->ors as $expression ) {
            $nextCond = $this->explorCond ( $expression );
            if($nextCond){
                $this->selectConds->addNextCond ( $nextCond, self::COND_OR );
            }
        }
        $lastCond = $this->selectConds;
        // 分析逻辑处理
        $boolTerm = new BoolTerm();
        while ( ! empty ( $lastCond ) ) {
            $this->processCond ( $lastCond, $boolTerm,$lastCond->nextCondType );
            $lastCond = $lastCond->nextCond;
        }
        // 设置表名
        $params = array (
            "index" => join ( ",", $this->indexs ) 
        );
        // 设置参数
        $params ["body"] ["query"] = $boolTerm->toArray();
        // 设置排序
        if (! empty ( $this->orders )) {
            foreach ( $this->orders as $field => $sort ) {
                $params ["body"] ["sort"] [] = [ 
                    $field => [ 
                        "order" => strtolower ( $sort ) 
                    ] 
                ];
            }
        }
        // 设置返回属性
        if (! empty ( $this->colums ) && $this->colums != "*") {
            $params ["_source"] = explode ( ",", $this->colums );
        }
        // 设置启始记录
        if ($this->skip > 0) {
            $params ["from"] = $this->skip;
        }
        // 设置页大小
        if ($this->limit > 0) {
            $params ["size"] = $this->limit;
        }
        $result ["Params"] = $params;
        return $result;
    }

    /**
     * 组装查询条件
     * @param SelectConds $cond            
     * @return Term
     */
    private function initTerm($cond)
    {
        $field = $cond->field;
        if (isset ( $this->binds [$cond->value] )) {
            $value = $this->binds [$cond->value];
        } else {
            $value = $cond->value;
        }
        switch (strtoupper ( $cond->op )) {
            case self::OP_EQ :
                return new MatchTerm ( $field, $value );
            case self::OP_NEQ :
                $bt = new BoolTerm ();
                $term = new MatchTerm ( $field, $value );
                $bt->addMustNot ( $term );
                return $bt;
            case self::OP_GT :
                return new RangeTerm ( $field, RangeTerm::OP_GT, $value );
            case self::OP_IN :
                return new InTerm ( $field, $value );
            case self::OP_NOT_IN :
                $term = new InTerm ( $field, $value );
                $bt = new BoolTerm ();
                $bt->addMustNot ( $term );
                return $bt;
            case self::OP_LT :
                return new RangeTerm ( $field, RangeTerm::OP_LT, $value );
            case self::OP_GTEQ :
                return new RangeTerm ( $field, RangeTerm::OP_GTE, $value );
            case self::OP_LTEQ :
                return new RangeTerm ( $field, RangeTerm::OP_LTE, $value );
            case self::OP_IS_NOT_NULL :
                return new ExistsTerm ( $field );
            case self::OP_IS_NULL :
                $bt = new BoolTerm ();
                $term = new ExistsTerm ( $field );
                $bt->addMustNot ( $term );
                return $bt;
                break;
            case self::OP_LIKE :
                if (preg_match ( "/%/", $value )) {
                    $value = str_replace ( "%", "*", $value );
                    return new WildcardTerm ( $field, $value );
                } else {
                    return new MatchTerm ( $field, $value );
                }
        }
    }

    /**
     * 转批条件
     *
     * @param SelectConds $cond 查询条件
     * @param BoolTerm $boolTerm 存放条件的boolQuery数组
     */
    private function processCond($cond, &$boolTerm, $type = self::COND_AND)
    {
        if ($cond->field ) {
            $term = $this->initTerm ( $cond, $type );
            if ($type == self::COND_AND) {
                if ($cond->op == self::OP_NEQ || $cond->op == self::OP_NOT_IN || $cond->op == self::OP_IS_NULL) {
                    $boolTerm->addMustnot($term);
                } else {
                    $boolTerm->addMust($term);
                }
            } else {
                $boolTerm->addShould($term);
            }
        }
        if ($cond->childCond) {
            $lastCond = $cond->childCond;
            $childTerm = new BoolTerm();
            while ( ! empty ( $lastCond ) ) {
                $this->processCond ( $lastCond, $childTerm,$lastCond->nextCondType );
                $lastCond = $lastCond->nextCond;
            }
            if ($type == self::COND_AND) {
                $boolTerm->addMust($childTerm);
            } else {
                $boolTerm->addShould($childTerm);
            }
        }
    }

    /**
     * 转化条件操作
     *
     * @param string $field
     *            属性
     * @param string $op
     *            操作符
     * @param string $value
     *            值
     * @return string
     */
    private function processExchangeCond($field, $op, $value)
    {
        switch (strtoupper ( $op )) {
            case self::OP_LIKE :
                // 模糊匹配的
                if (preg_match ( "/%/i", $value )) {
                    $value = str_replace ( "%", "*", $value );
                    $this->wildcard [] = [ 
                        $field => $value 
                    ];
                } else {
                    // 精确查询
                    $this->matchQuery [] = [ 
                        $field => $value 
                    ];
                }
                break;
            case self::OP_IS_NULL :
                return " attribute_not_exists(" . $field . ")";
                break;
            case self::OP_IS_NOT_NULL :
                return " attribute_exists(" . $field . ")";
                break;
            case self::OP_EQ :
                $this->matchQuery [] = [ 
                    $field => $value 
                ];
                break;
            case self::OP_NEQ :
                $this->matchQuery [] = [ 
                    $field => $value 
                ];
            default :
                return $field . " " . $op . " " . $value;
        }
    }

    /*
     * (non-PHPdoc) @see \Duomai\Db\Select\Select::checkCond()
     */
    protected function checkCond()
    {
        return true;
    }
}