<?php
namespace Nosql\Select;

use Nosql\DbException;
use Nosql\Adapter\AbstractDb;

/**
 * 组织转义DynamoDB的查询条件
 */
abstract class Select
{
    const COND_AND = "AND";
    const COND_OR = "OR";
    
    // 支持的查询操作
    const OP_EQ = "=";
    const OP_NEQ = "!=";
    const OP_LT = "<";
    const OP_LTEQ = "<=";
    const OP_GT = ">";
    const OP_GTEQ = ">=";
    const OP_IN = "IN";
    const OP_NOT_IN = "NOT IN";
    const OP_LIKE = "LIKE";
    const OP_IS_NULL = "IS NULL";
    const OP_IS_NOT_NULL = "IS NOT NULL";
//     const OP_BETWEEN = "BETWEEN"; //暂不支持，
    
    /**
     * 合并后的查询条件
     * @var array
     */
    protected  $query =[];
    /**
     * and条件
     * 
     * @var array 结构如：
     *      array(
     *      array(
     *      "Field"=>"domain_name",
     *      "Op"=>"=",
     *      "Value"=>"dn"
     *      ),
     *      )
     */
    protected $andCond = array ();
    /**
     * 嵌套的查询条件:如 Field=:p1: and (Field=:p2: or Field=:p3:) 中括号里的条件，最终会以and方式合并到整体的查询条件中，目前只支持一级嵌套
     * 
     * @var array
     */
    protected $subAndCond = array ();
    /**
     * or 条件
     * 
     * @var array 结构如：
     *      array(
     *      array(
     *      "Field"=>"domain_name",
     *      "Op"=>"=",
     *      "Value"=>"dn"
     *      ),
     *      )
     */
    protected $orCond = array ();
    /**
     * 嵌套的查询条件:如 Field=:p1: and (Field=:p2: or Field=:p3:) 中括号里的条件，最终会以and方式合并到整体的查询条件中，目前只支持一级嵌套
     * 
     * @var array
     */
    protected $subOrCond = array ();
    /**
     * 拆分后的过滤条件数组
     * @var array
     */
    protected $FilterExpression = [];
    /**
     * 同FilterExpression，只是是嵌套的
     * 
     * @var array
     */
    protected $SubFilterExpression = [ ];
    
    
    /**
     * 错误信息记录
     * 
     * @var string
     */
    public $errors;
    /**
     * 表名
     * 
     * @var string
     */
    public $tableName;
    /**
     * 属性的映射定义，用于转义
     * 
     * @var array
     */
    public $attrMapping;
    
    /**
     * 要返回的属性
     * 
     * @var mixed
     */
    public $colums = "*";
    
    // 没有having，没有group，
    /**
     * and操作记录
     * 
     * @var array
     */
    protected $ands = array ();
    /**
     * or操作记录
     * 
     * @var array
     */
    protected $ors = array ();
    /**
     * 绑定参数
     * 
     * @var array
     */
    protected $binds = array ();
    
    /**
     * 排序操作记录
     * 
     * @var array
     */
    protected $orders = array ();
    
    /**
     * 一次查询大小
     * 
     * @var int
     */
    protected $limit = 20;
    
    /**
     * 跳过的条数
     * 
     * @var int
     */
    protected $skip = 0;
    
    /**
     * 数据库适配器
     * 
     * @var AbstractDb
     */
    protected $adapter;

    /**
     * 构造函数
     * 
     * @param AbstractDb $adapter            
     */
    public function __construct(AbstractDb $adapter)
    {
        $this->adapter = $adapter;
    }

    /**
     *
     * @return the $errors
     */
    public function getErrors()
    {
        return $this->errors;
    }

    /**
     *
     * @return the $binds
     */
    public function getBinds()
    {
        return $this->binds;
    }

    /**
     *
     * @return the $orders
     */
    public function getOrders()
    {
        return $this->orders;
    }

    /**
     *
     * @return the $limit
     */
    public function getLimit()
    {
        return $this->limit;
    }

    /**
     *
     * @return the $skip
     */
    public function getSkip()
    {
        return $this->skip;
    }

    /**
     * 返回查询的表名
     * 
     * @return string
     */
    public function getTableName()
    {
        return $this->tableName;
    }

    /**
     *
     * @return the $colums
     */
    public function getColums()
    {
        return $this->colums;
    }

    /**
     *
     * @return the $adapter
     */
    public function getAdapter()
    {
        return $this->adapter;
    }

    /**
     * 从哪个表里查数据
     * 
     * @param string $tableName            
     * @return Select
     */
    abstract public function from($tableName);

    /**
     * 需要返回的属性
     * 
     * @param string|Array $columns
     *            *表示查所有属性
     * @return Select
     */
    public function columns($columns = "*")
    {
        $this->colums = $columns;
        return $this;
    }

    /**
     * 设置属性转义
     * 例：Field跟服务的关键词冲突，则需要定义array("Field","myField");这种方式，意思是把Field用别名myField代替
     * 
     * @param array $attrMapping            
     * @return Select
     */
    public function setAttrMapping($attrMapping)
    {
        $this->attrMapping = $attrMapping;
        return $this;
    }

    /**
     * 查询条件，and查询
     * 
     * @param string $cond
     *            查询表达式
     *            例：Field=:p1: And Field!=:p2: 其中p1，跟p2就是条件占位符，在参数bind中指定其值。
     * @param array $bind
     *            绑定的参数,由cond中的参数占位符指定
     *            例：array("p1"=>"sdfsdfsd","p2"=>"sdfsdfsdf")
     * @return Select
     */
    public function where($cond, $bind=null)
    {
        $this->ands [] = $cond;
        if(!empty($bind)){
            $this->binds = array_merge ( $this->binds, $bind );
        }
        return $this;
    }

    /**
     * 查询条件 or查询，
     * 
     * @param string $cond
     *            查询表达式
     *            例：Field=:p1: And Field!=:p2: 其中p1，跟p2就是条件占位符，在参数bind中指定其值。
     * @param array $bind
     *            绑定的参数,由cond中的参数占位符指定
     *            例：array("p1"=>"sdfsdfsd","p2"=>"sdfsdfsdf")
     * @return Select
     */
    public function orWhere($cond, $bind)
    {
        $this->ors [] = $cond;
        $this->binds = array_merge ( $this->binds, $bind );
        return $this;
    }

    /**
     * and 的 in 操作
     * 
     * @param string $field            
     * @param array $values            
     * @return Select
     */
    public function inWhere($field, $values)
    {
        $alias = "in_" . $field;
        $cond = $field . " in (:" . $alias . ":)";
        $isStr = false; // 默认都按字符串处理
        foreach ( $values as $v ) {
            if (is_int ( $v ) || is_float ( $v ) || is_bool ( $v )) {
                continue;
            }
            $isStr = true;
            break;
        }
        if ($isStr) {
            $bind = array (
                $alias => join ( ",", $values ) 
            );
        } else {
            foreach ( $values as $k => $v ) {
                $v = "'" . $this->getAdapter ()->quoteStr ( $v ) . "'";
                $values [$k] = $v;
            }
        }
        $this->ands [] = preg_replace ( "/:" . $alias . ":/", "(" . join ( ",", $values ) . ")", $cond );
        return $this;
    }

    /**
     * 排序操作
     * 例：array("ItemID"=>"Desc","CreateTime"=>"ASC");
     * 
     * @param array $order            
     * @return Select
     */
    public function orderBy($order)
    {
        $this->orders = array_merge ( $this->orders, $order );
        return $this;
    }

    /**
     * 限制查询条数
     * 
     * @param int $limit            
     * @param mixed $skip
     *            跳过的条数，或起始数据
     * @return Select
     */
    public function limit($limit, $skip = null)
    {
        $this->limit = $limit;
        $this->skip = $skip;
        return $this;
    }

    /**
     * 附加错误信息
     * 
     * @param unknown $msg            
     */
    public function error($msg)
    {
        $this->errors .= " " . $msg;
    }

    /**
     * 查询前的检查操作
     */
    protected abstract function checkCond();

    /**
     * 根据设置的条件生成具体数据库适配的查询格式
     * 
     * @return mixed 不同数据库的格式不一样
     */
    protected abstract function assemble();

    /**
     * 返回直接用于接口的查询的参数格式
     * 
     * @throws DbException
     * @return mixed 不同数据库的格式不一样
     */
    public function getQuery()
    {
        if(!empty($this->query)){
            return $this->query;
        }
        if ($this->checkCond ()) {
            $this->query =  $this->assemble ();
            return $this->query;
        } else {
            throw new DbException ( $this->errors );
        }
    }
    
    /**
     * 按优先级拆分查询语句,嵌套的or操作只能filter处理
     *
     * @param string $expression
     *            语句
     * @param string $cond
     *            and 或or
     * @param int $subIndex
     *            嵌套查询索引,-1表示非嵌套查询
     * @return array 单个语句的数组组合
     */
    protected function explorCond($expression, $cond = self::COND_AND,$subIndex = -1)
    {
        $expression = trim ( $expression );
        if ($expression) {
            if (preg_match ( "/^\((?<subExp>.*)\)$/", $expression, $match )) {
                // 嵌套查询处理
                $this->explorCond ( $match ["subExp"], $cond, $subIndex + 1 );
                return;
            }
            $strupper = strtoupper ( $expression );
            $opPreg = array (
                self::OP_EQ,
                self::OP_GT,
                self::OP_GTEQ,
                self::OP_IN,
                self::OP_NOT_IN,
                self::OP_IS_NOT_NULL,
                self::OP_IS_NULL,
                self::OP_LIKE,
                self::OP_LT,
                self::OP_LTEQ,
                self::OP_NEQ
            );
            $opPreg = "(" . join ( ")|(", $opPreg ) . ")";
            // 按优先级，按and先分
            if (strpos ( $strupper, " " . self::COND_AND . " " ) > 0) {
                $pos = strpos ( $strupper, " " . self::COND_AND . " " );
                $left = substr ( $expression, 0, $pos );
                $this->explorCond ( $left, self::COND_AND,$subIndex );
                $right = substr ( $expression, $pos + 4 );
                $this->explorCond ( $right, self::COND_AND,$subIndex );
            } elseif (strpos ( $strupper, " " . self::COND_OR . " " ) > 0) {
                $pos = strpos ( $strupper, " " . self::COND_OR . " " );
                $left = substr ( $expression, 0, $pos );
                $this->explorCond ( $left, self::COND_OR,$subIndex );
                $right = substr ( $expression, $pos + 3 );
                $this->explorCond ( $right, self::COND_OR,$subIndex );
            } elseif (preg_match ( "/^(?<field>[\w\d_]+)\s*(?<op>" . $opPreg . ")\s*(?<val>:?[\w\d_]+:)?$/i", $expression, $match )) {
                if ($cond == self::COND_OR) {
                    if($subIndex>=0){
                        $this->subOrCond[$subIndex] [] = array (
                            "Field" => $match ["field"],
                            "Op" => $match ["op"],
                            "Value" => isset ( $match ["val"] ) ? trim($match ["val"],":") : ''
                        );
                    }else{
                        $this->orCond [] = array (
                            "Field" => $match ["field"],
                            "Op" => $match ["op"],
                            "Value" => isset ( $match ["val"] ) ? trim($match ["val"],":") : ''
                        );
                    }
                } elseif ($cond == self::COND_AND) {
                    if($subIndex>=0){
                        $this->subAndCond[$subIndex] [] = array (
                            "Field" => $match ["field"],
                            "Op" => $match ["op"],
                            "Value" => isset ( $match ["val"] ) ? trim($match ["val"],":") : ''
                        );
                    }else{
                        $this->andCond [] = array (
                            "Field" => $match ["field"],
                            "Op" => $match ["op"],
                            "Value" => isset ( $match ["val"] ) ? trim($match ["val"],":") : ''
                        );
                    }
                }
                return;
            } else {
                if($subIndex>=0){
                    // 放入filter
                    if (! empty ( $this->FilterExpression )) {
                        $this->SubFilterExpression[$subIndex][]= " " . $cond . " " . $expression;
                    } else {
                        $this->SubFilterExpression[$subIndex][] = $expression;
                    }
                }else{
                    // 放入filter
                    if (! empty ( $this->FilterExpression )) {
                        $this->FilterExpression []= " " . $cond . " " . $expression;
                    } else {
                        $this->FilterExpression[] = $expression;
                    }
                }
            }
        } else {
            throw new DbException ( "invalie condition" );
        }
    }

}