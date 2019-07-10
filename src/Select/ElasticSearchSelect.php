<?php
namespace Nosql\Select;

use Nosql\Select\Select;
use Nosql\DbException;

class ElasticSearchSelect extends Select
{
    /**
     * 多个索引同时搜索
     * @var array
     */
    private $indexs = [];
    
    private $rangeAnd=[];

    /**
     * 模糊匹配查询
     */
    private $wildcardAnd=[];
    private $wildcardOr=[];
    
    private $subWildcardAnd=[];
    private $subWildcardOr=[];
    
    /**
     * 精确查询
     * @var array
     */
    private $termQueryAnd=[];
    private $termQueryOr=[];
    
    /**
     * 嵌套精确查询
     * @var array
     */
    private $subTermQueryAnd=[];
    private $subTermQueryOr=[];
    
    
    /*
     * (non-PHPdoc)
     * @see \Duomai\Db\Select\Select::from()
     */
    public function from($indexName)
    {
        if(!in_array($indexName, $this->indexs)){
            $this->indexs[] = $indexName;
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
            $this->explorCond ( $expression, self::COND_AND );
        }
        foreach ( $this->ors as $expression ) {
            $this->explorCond ( $expression, self::COND_OR );
        }
        // 分析逻辑处理
        foreach ( $this->andCond as $cond ) {
            $this->processAndCond($cond, $this->termQueryAnd);
        }
        foreach ( $this->orCond as $cond ) {
            $this->processOrCond($cond, $this->termQueryOr);
        }
        $subAndConds = [];
        foreach ($this->subAndCond as $idx=>$subConds){
            $tmpCond = [];
            foreach ($subConds as $cond){
                $this->processAndCond($cond, $tmpCond);
            }
            if(!empty($tmpCond)){
                $subAndConds[] = $tmpCond;
            }
        }
        $subOrConds = [];
        foreach ($this->subOrCond as $idx=>$subConds){
            $tmpCond = [];
            foreach ($subConds as $cond){
                $this->processOrCond($cond, $tmpCond);
            }
            if(!empty($tmpCond)){
                $subOrConds[] = $tmpCond;
            }
        }
        
        if(!empty($subAndConds)){
            //嵌套的and操作。也是and操作
            foreach ($subAndConds as $type=>$conds){
                if(isset($this->termQueryAnd[$type])){
                    $this->termQueryAnd[$type] = array_merge($this->termQueryAnd[$type],$conds);
                }else{
                    $this->termQueryAnd[$type] = $conds;
                }
            }
        }
        if(!empty($subOrConds)){
            //嵌套的or操作，放terms里
            //TODO
        }
        
        // 设置表名
        $params = array (
            "index" => join(",", $this->indexs)
        );
        // 设置参数
        if (!empty($this->termQueryAnd)){
            $params["body"]["query"]["bool"]=$this->termQueryAnd;
        }
        if (!empty($this->termQueryOr)){
            //TODO
        }
        // 设置排序
        if (! empty ( $this->orders )) {
            foreach ($this->orders as $field=>$sort){
                $params["sort"][]=[$field=>["order"=>strtolower($sort)]];
            }
        }
        // 设置返回属性
        if (! empty ( $this->colums )&&$this->colums!="*") {
            $params["_source"] = explode(",", $this->colums);
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
     * @param array $cond 查询条件
     * @param array $resultArr 存放条件的数组
     */
    private  function processAndCond($cond,&$resultArr){
        $field = $cond ["Field"];
        if(isset($this->binds[$cond["Value"]])){
            $value = $this->binds[$cond["Value"]];
        }else{
            $value="";
        }
        switch (strtoupper($cond["Op"])){
            case self::OP_EQ:
                $resultArr["must"][]=["term"=>[$field=>$value]];
                break;
            case self::OP_NEQ:
                $resultArr["must_not"][]=["term"=>[$field=>$value]];
                break;
            case self::OP_GT:
                $resultArr["must"][]=["range"=>[$field=>["gt"=>$value]]];
                break;
            case self::OP_LT:
                $resultArr["must"][]=["range"=>[$field=>["lt"=>$value]]];
                break;
            case self::OP_GTEQ:
                $resultArr["must"][]=["range"=>[$field=>["gte"=>$value]]];
                break;
            case self::OP_LTEQ:
                $resultArr["must"][]=["range"=>[$field=>["lte"=>$value]]];
                break;
            case self::OP_IS_NOT_NULL:
                $resultArr["must"][]=["exists"=>["field"=>$field]];
                break;
            case self::OP_IS_NULL:
                $resultArr["must_not"][]=["exists"=>["field"=>$field]];
                break;
            case self::OP_LIKE:
                if(preg_match("/%/", $value)){
                    $value = str_replace("%", "*", $value);
                    $resultArr["must"][]=["wildcard"=>[$field=>$value]];
                }else{
                    $resultArr["must"][]=["term"=>[$field=>$value]];
                }
                break;
        }
    }
    /**
     * 转批条件
     * @param array $cond 查询条件
     * @param array $resultArr 存放条件的数组
     */
    private  function processOrCond($cond,&$resultArr){
        $field = $cond ["Field"];
        if(isset($this->binds[$cond["Value"]])){
            $value = $this->binds[$cond["Value"]];
        }else{
            $value="";
        }
        switch (strtoupper($cond["Op"])){
            case self::OP_EQ:
                $resultArr["must"][]=["term"=>[$field=>$value]];
                break;
            case self::OP_NEQ:
                $resultArr["must_not"][]=["term"=>[$field=>$value]];
                break;
            case self::OP_GT:
                $resultArr["must"][]=["range"=>[$field=>["gt"=>$value]]];
                break;
            case self::OP_LT:
                $resultArr["must"][]=["range"=>[$field=>["lt"=>$value]]];
                break;
            case self::OP_GTEQ:
                $resultArr["must"][]=["range"=>[$field=>["gte"=>$value]]];
                break;
            case self::OP_LTEQ:
                $resultArr["must"][]=["range"=>[$field=>["lte"=>$value]]];
                break;
            case self::OP_IS_NOT_NULL:
                $resultArr["must"][]=["exists"=>["field"=>$field]];
                break;
            case self::OP_IS_NULL:
                $resultArr["must_not"][]=["exists"=>["field"=>$field]];
                break;
            case self::OP_LIKE:
                if(preg_match("/%/", $value)){
                    $value = str_replace("%", "*", $value);
                    $resultArr["must"][]=["wildcard"=>[$field=>$value]];
                }else{
                    $resultArr["must"][]=["term"=>[$field=>$value]];
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
                //模糊匹配的
                if (preg_match ( "/%/i", $value )) {
                    $value = str_replace("%", "*", $value);
                    $this->wildcard[]=[$field=>$value];
                } else {
                    //精确查询
                    $this->matchQuery[] = [$field=>$value];
                }
                break;
            case self::OP_IS_NULL :
                return " attribute_not_exists(" . $field . ")";
                break;
            case self::OP_IS_NOT_NULL :
                return " attribute_exists(" . $field . ")";
                break;
            case self::OP_EQ:
                $this->matchQuery[] = [$field=>$value];
                break;
            case self::OP_NEQ:
                $this->matchQuery[] = [$field=>$value];
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