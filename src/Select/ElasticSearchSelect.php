<?php
namespace Nosql\Select;

use Nosql\Select\Select;

class ElasticSearchSelect extends Select
{
    /**
     * 多个索引同时搜索
     *
     * @var array
     */
    private $indexs = [ ];
    
    /**
     * 精确查询
     *
     * @var array
     */
    private $termQueryAnd = [ ];
    private $termQueryOr = [ ];
    
    /**
     * 嵌套查询
     *
     * @var array
     */
    private $subTermQueryAnd = [ ];
    private $subTermQueryOr = [ ];

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
            $this->selectConds->addNextCond($this->explorCond ( $expression, self::COND_AND ), self::COND_AND);
        }
        foreach ( $this->ors as $expression ) {
            $this->selectConds->addNextCond($this->explorCond ( $expression, self::COND_OR ),self::COND_OR);
        }
        $lastCond = $this->selectConds;
        var_dump($lastCond);
        exit;
        // 分析逻辑处理
        $boolQuery = [];
        while (!empty($lastCond)){
            $this->processCond ( $lastCond,$query );
            $lastCond = $this->selectConds->nextCond;
        }
        
        // 设置表名
        $params = array (
            "index" => join ( ",", $this->indexs ) 
        );
        // 设置参数
        if (! empty ( $boolQuery )) {
            $params ["body"] ["query"] ["bool"] = $boolQuery;
        }
        // 设置排序
        if (! empty ( $this->orders )) {
            foreach ( $this->orders as $field => $sort ) {
                $params["body"]["sort"] [] = [ 
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
     * 转批条件
     *
     * @param SelectConds $cond  查询条件
     * @param array $resultArr 存放条件的boolQuery数组
     */
    private function processCond($cond, &$resultArr)
    {
        $field = $cond->field;
        if (isset ( $this->binds [$cond->value] )) {
            $value = $this->binds [$cond->value];
        } else {
            $value = $cond->value;
        }
        switch (strtoupper ( $cond->op )) {
            case self::OP_EQ :
                $resultArr ["must"] [] = [ 
                    "term" => [ 
                        $field => $value 
                    ] 
                ];
                break;
            case self::OP_NEQ :
                $resultArr ["must_not"] [] = [ 
                    "term" => [ 
                        $field => $value 
                    ] 
                ];
                break;
            case self::OP_GT :
                $resultArr ["must"] [] = [ 
                    "range" => [ 
                        $field => [ 
                            "gt" => $value 
                        ] 
                    ] 
                ];
                break;
            case self::OP_IN :
                $resultArr ["must"] [] = [ 
                    "terms" => [ 
                        $field => $value 
                    ] 
                ];
                break;
            case self::OP_NOT_IN :
                $resultArr ["must_not"] [] = [ 
                    "terms" => [ 
                        $field => $value 
                    ] 
                ];
                break;
            case self::OP_LT :
                $resultArr ["must"] [] = [ 
                    "range" => [ 
                        $field => [ 
                            "lt" => $value 
                        ] 
                    ] 
                ];
                break;
            case self::OP_GTEQ :
                $resultArr ["must"] [] = [ 
                    "range" => [ 
                        $field => [ 
                            "gte" => $value 
                        ] 
                    ] 
                ];
                break;
            case self::OP_LTEQ :
                $resultArr ["must"] [] = [ 
                    "range" => [ 
                        $field => [ 
                            "lte" => $value 
                        ] 
                    ] 
                ];
                break;
            case self::OP_IS_NOT_NULL :
                $resultArr ["must"] [] = [ 
                    "exists" => [ 
                        "field" => $field 
                    ] 
                ];
                break;
            case self::OP_IS_NULL :
                $resultArr ["must_not"] [] = [ 
                    "exists" => [ 
                        "field" => $field 
                    ] 
                ];
                break;
            case self::OP_LIKE :
                if (preg_match ( "/%/", $value )) {
                    $value = str_replace ( "%", "*", $value );
                    $resultArr ["must"] [] = [ 
                        "wildcard" => [ 
                            $field => $value 
                        ] 
                    ];
                } else {
                    $resultArr ["must"] [] = [ 
                        "term" => [ 
                            $field => $value 
                        ] 
                    ];
                }
                break;
        }
    }

    /**
     * 转批条件
     *
     * @param array $cond
     *            查询条件
     * @param array $resultArr
     *            存放条件的数组
     */
    private function processOrCond($cond, &$resultArr)
    {
        $field = $cond ["Field"];
        if (isset ( $this->binds [$cond ["Value"]] )) {
            $value = $this->binds [$cond ["Value"]];
        } else {
            $value = "";
        }
        switch (strtoupper ( $cond ["Op"] )) {
            case self::OP_EQ :
                $resultArr ["should"] [] = [ 
                    "term" => [ 
                        $field => $value 
                    ] 
                ];
                break;
            case self::OP_NEQ :
                $resultArr ["should"] [] = [ 
                    "bool" => [ 
                        "must_not" => [ 
                            "term" => [ 
                                $field => $value 
                            ] 
                        ] 
                    ] 
                ];
                break;
            case self::OP_GT :
                $resultArr ["should"] [] = [ 
                    "range" => [ 
                        $field => [ 
                            "gt" => $value 
                        ] 
                    ] 
                ];
                break;
            case self::OP_IN :
                $resultArr ["should"] [] = [ 
                    "terms" => [ 
                        $field => $value 
                    ] 
                ];
                break;
            case self::OP_NOT_IN :
                $resultArr ["should"] [] = [ 
                    "bool" => [ 
                        "must_not" => [ 
                            "terms" => [ 
                                $field => $value 
                            ] 
                        ] 
                    ] 
                ];
                break;
            case self::OP_LT :
                $resultArr ["should"] [] = [ 
                    "range" => [ 
                        $field => [ 
                            "lt" => $value 
                        ] 
                    ] 
                ];
                break;
            case self::OP_GTEQ :
                $resultArr ["should"] [] = [ 
                    "range" => [ 
                        $field => [ 
                            "gte" => $value 
                        ] 
                    ] 
                ];
                break;
            case self::OP_LTEQ :
                $resultArr ["should"] [] = [ 
                    "range" => [ 
                        $field => [ 
                            "lte" => $value 
                        ] 
                    ] 
                ];
                break;
            case self::OP_IS_NOT_NULL :
                $resultArr ["should"] [] = [ 
                    "exists" => [ 
                        "field" => $field 
                    ] 
                ];
                break;
            case self::OP_IS_NULL :
                $resultArr ["should"] [] = [ 
                    "bool" => [ 
                        "must_not" => [ 
                            "exists" => [ 
                                "field" => $field 
                            ] 
                        ] 
                    ] 
                ];
                break;
            case self::OP_LIKE :
                if (preg_match ( "/%/", $value )) {
                    $value = str_replace ( "%", "*", $value );
                    $resultArr ["should"] [] = [ 
                        "wildcard" => [ 
                            $field => $value 
                        ] 
                    ];
                } else {
                    $resultArr ["should"] [] = [ 
                        "term" => [ 
                            $field => $value 
                        ] 
                    ];
                }
                break;
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