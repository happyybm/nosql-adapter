<?php
namespace Nosql\Select;

use Nosql\Select\Select;
use Aws\DynamoDb\Marshaler;
use Nosql\DbException;

/**
 * DynamoSelect结构化查询处理
 */
class DynamoSelect extends Select
{
    static $METHOD_QUERY = "Query";
    static $METHOD_SCAN = "Scan";
    
    private $hasSetIndexKeyCondition = true;
    
    /*
     * 通用条件
     * TableName 表名
     * ConsistentRead 是否强一致性读取，取一条数据时，要求强一致性？
     * ProjectionExpression 要返回的属性
     * ExpressionAttributeNames 属性别名 如{"#P":"Percentile"},可指定多个
     * ExpressionAttributeValues 属性值：如：{ ":avail":{"S":"Available"}, ":back":{"S":"Backordered"}, ":disc":{"S":"Discontinued"} }
     *
     * Query 按主键查询多条item
     * Limit 要返回的条数，结合ExclusiveStartKey提供翻页效果
     * ExclusiveStartKey 启始索引，当查询超过吞吐量时，要经过多次查询才能返回整个结果。
     * ScanIndexForward 排序操作 true 正序，false：倒序，是按排序索引排的
     * IndexName 指定查询的索引
     * KeyConditionExpression 只能是主键的查询条件,如：partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval
     * FilterExpression 只能是非索引的属性的过滤，过滤操作是要查询结果后再操作的，所以即使过滤这些item同样也是耗费资源的
     *
     * Scan 非按主键的查询,scan时没有排序操作，排序只能对索引进行排序
     * Limit 要返回的条数，结合ExclusiveStartKey提供翻页效果
     * ExclusiveStartKey 启始索引，当查询超过吞吐量时，要经过多次查询才能返回整个结果。
     * FilterExpression 只能是非索引的属性的过滤，过滤操作是要查询结果后再操作的，所以即使过滤这些item同样也是耗费资源的
     * Segment 并行查询时指定的进程号 默认从0开始，如果结合ExclusiveStartKey的话，这个Segment也要跟上，这样分页才对。
     * TotalSegments 指定并行查询的进程数，1～1000000之间 跟参数 Segment一起用
     */
    private $IndexName = "";
    private $Segment = 0;
    private $TotalSegments = 0;
    private $KeyConditionExpression = "";
    private $FilterExpression = array ();
    private $ConsistentRead = false;
    private $ProjectionExpression = array ();
    private $ExpressionAttributeNames = array ();
    private $ExpressionAttributeValues = array ();
    private $ExclusiveStartKey = null;
    private $ScanIndexForward = null;
    
    /**
     * 表信息：属性，主键，索引
     * 分区键查询只能适用=操作，
     * TODO 这里还未区分分区键，二级索引等，待优化
     * 
     * @var array
     */
    private $tableInfo = array (
        "Fields" => array (),
        "Keys" => array (),
        "Indexs" => array () 
    );

    /**
     *
     * @return the $ExclusiveStartKey
     */
    public function getExclusiveStartKey()
    {
        return $this->ExclusiveStartKey;
    }

    /**
     *
     * @param field_type $ExclusiveStartKey            
     */
    public function setExclusiveStartKey($ExclusiveStartKey)
    {
        $this->ExclusiveStartKey = $ExclusiveStartKey;
    }

    /**
     * from默认指定表名，再调用该方法时就指定索引名
     * 
     * @param string $tableName
     *            表名或索引名
     */
    public function from($tableName)
    {
        if (empty ( $this->tableName )) {
            $this->tableName = $tableName;
        } else {
            $this->IndexName = $tableName;
            $this->hasSetIndexKeyCondition = false;
        }
        return $this;
    }

    /**
     * 清除原有的查询条件
     */
    protected function reset()
    {
        $this->ProjectionExpression = array ();
        $this->ExpressionAttributeNames = array ();
        $this->ExpressionAttributeValues = array ();
        $this->FilterExpression = array ();
        $this->orCond = array ();
        $this->andCond = array ();
        $this->KeyConditionExpression = "";
        $this->hasSetIndexKeyCondition = true;
    }

    /**
     * 根据不同的条件，指定使用的查询方式：Query,Scan
     * 
     * @return array $query
     *         例：
     *         array(
     *         "Method"=>"Query",//需要调用的接口
     *         "Params"=>array()//接口参数
     *         )
     */
    protected function assemble()
    {
        $marsh = new Marshaler ();
        $this->reset ();
        // 处理转义返回字段
        if (! empty ( $this->colums ) && $this->colums != "*") {
            // 别名替换
            foreach ( $this->colums as $field ) {
                $this->ProjectionExpression [] = self::getExchangeField ( $field );
                $this->ExpressionAttributeNames [self::getExchangeField ( $field )] = $field;
            }
        }
        // 处理绑定参数
        if (! empty ( $this->binds )) {
            foreach ( $this->binds as $k => $v ) {
                $this->ExpressionAttributeValues [":" . $k] = $v;
            }
        }
        // 分析查询逻辑条件
        foreach ( $this->ands as $expression ) {
            $this->explorCond ( $expression, self::COND_AND );
        }
        foreach ( $this->ors as $expression ) {
            $this->explorCond ( $expression, self::COND_OR );
        }
        // 分析逻辑处理，指定query或scan方式查询
        foreach ( $this->andCond as $cond ) {
            $field = $cond ["Field"];
            // 把条件语句中的字段转义
            $this->ExpressionAttributeNames [self::getExchangeField ( $field )] = $field;
            $cond = $this->getExchangeCond ( self::getExchangeField ( $field ), $cond ["Op"], $cond ["Value"] );
            
            if (! $this->IndexName && in_array ( $field, $this->tableInfo ["Keys"] )) {
                if ($this->KeyConditionExpression == "") {
                    $this->KeyConditionExpression = $cond;
                } else {
                    $this->KeyConditionExpression .= " and " . $cond;
                }
            } else {
                if ($this->IndexName && isset ( $this->tableInfo [$this->IndexName] ["Indexs"] ) && in_array ( $field, $this->tableInfo [$this->IndexName] ["Indexs"] )) {
                    $this->hasSetIndexKeyCondition = true;
                    if ($this->KeyConditionExpression == "") {
                        $this->KeyConditionExpression = $cond;
                    } else {
                        $this->KeyConditionExpression .= " and " . $cond;
                    }
                } else {
                    if (! empty ( $this->FilterExpression )) {
                        $this->FilterExpression .= " and " . $cond;
                    } else {
                        $this->FilterExpression = $cond;
                    }
                }
            }
        }
        foreach ( $this->orCond as $cond ) {
            $field = $cond ["Field"];
            // 把条件语句中的字段转义
            $this->ExpressionAttributeNames [self::getExchangeField ( $field )] = $field;
            $cond = $this->getExchangeCond ( self::getExchangeField ( $field ), $cond ["Op"], $cond ["Value"] );
            if (! $this->IndexName && in_array ( $field, $this->tableInfo ["Keys"] )) {
                if ($this->KeyConditionExpression == "") {
                    $this->KeyConditionExpression = $cond;
                } else {
                    $this->KeyConditionExpression .= " or " . $cond;
                }
            } else {
                if ($this->IndexName && isset ( $this->tableInfo [$this->IndexName] ["Indexs"] ) && in_array ( $field, $this->tableInfo [$this->IndexName] ["Indexs"] )) {
                    $this->hasSetIndexKeyCondition = true;
                    if ($this->KeyConditionExpression == "") {
                        $this->KeyConditionExpression = $cond;
                    } else {
                        $this->KeyConditionExpression .= " or " . $cond;
                    }
                } else {
                    if (! empty ( $this->FilterExpression )) {
                        $this->FilterExpression .= " or " . $cond;
                    } else {
                        $this->FilterExpression = $cond;
                    }
                }
            }
        }
        
        // 设置参数
        // 设置表名,默认非强一致性读取
        $params = array (
            "TableName" => $this->getTableName (),
            "ConsistentRead" => $this->ConsistentRead 
        );
        // 检查query查询必须指定主键
        if (! $this->hasSetIndexKeyCondition) {
            throw new DbException ( "please check the index key condition" );
        }
        // 设置排序
        if (! empty ( $this->orders )) {
            if (count ( $this->tableInfo ["Keys"] ) > 1) {
                // 只能处理正序，倒序，且在有排序键的前提下
                $order = "asc";
                foreach ( $this->orders as $file => $o ) {
                    $order = strtolower ( $o );
                    break;
                }
                $params ["ScanIndexForward"] = ($order == "asc") ? true : false;
            } else {
                throw new DbException ( "can not order while the table has not range key" );
            }
        }
        // 设置返回属性
        if (! empty ( $this->ProjectionExpression )) {
            $params ["ProjectionExpression"] = join ( ",", $this->ProjectionExpression );
        }
        // 设置绑定参数
        if (! empty ( $this->ExpressionAttributeValues )) {
            $params ["ExpressionAttributeValues"] = $marsh->marshalItem ( $this->ExpressionAttributeValues );
        }
        // 设置映射关系
        if (! empty ( $this->ExpressionAttributeNames )) {
            $params ["ExpressionAttributeNames"] = $this->ExpressionAttributeNames;
        }
        // 设置页大小
        if ($this->limit > 0) {
            $params ["Limit"] = $this->limit;
        }
        // 设置启始记录
        if (! empty ( $this->ExclusiveStartKey )) {
            $params ["ExclusiveStartKey"] = $this->ExclusiveStartKey;
        }
        // 设置指定的索引
        if ($this->IndexName) {
            $params ["IndexName"] = $this->IndexName;
        }
        // 设置结果过滤条件
        if (! empty ( $this->FilterExpression )) {
            $params ["FilterExpression"] = $this->FilterExpression;
        }
        // 指定查询类型
        if (empty ( $this->KeyConditionExpression )) {
            $result ["Method"] = self::$METHOD_SCAN;
        } else {
            $params ["KeyConditionExpression"] = $this->KeyConditionExpression;
            $result ["Method"] = self::$METHOD_QUERY;
        }
        $result ["Params"] = $params;
        return $result;
    }

    /**
     * 转义字段，前面加#号
     * 
     * @param string $field            
     * @return string
     */
    public static function getExchangeField($field)
    {
        return "#" . $field;
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
    private function getExchangeCond($field, $op, $value)
    {
        $realValue = $value;
        if (preg_match ( "/^:.*/", $value )) {
            $realValue = $this->ExpressionAttributeValues [$value];
        }
        switch (strtoupper ( $op )) {
            case self::OP_LIKE :
                if (preg_match ( "/^%(.*)%$/i", $realValue )) {
                    $realValue = trim ( $realValue, "%" );
                    if (isset ( $this->ExpressionAttributeValues [$value] )) {
                        $this->ExpressionAttributeValues [$value] = $realValue;
                    }
                    return "contains(" . $field . "," . $value . ")";
                } elseif (preg_match ( "/^(.*)%$/i", $realValue )) {
                    $realValue = trim ( $realValue, "%" );
                    if (isset ( $this->ExpressionAttributeValues [$value] )) {
                        $this->ExpressionAttributeValues [$value] = $realValue;
                    }
                    return "begins_with(" . $field . "," . $value . ")";
                } elseif (preg_match ( "/^%(.*)$/i", $realValue )) {
                    $realValue = trim ( $realValue, "%" );
                    if (isset ( $this->ExpressionAttributeValues [$value] )) {
                        $this->ExpressionAttributeValues [$value] = $realValue;
                    }
                    return "contains(" . $field . "," . $value . ")";
                } else {
                    return $field . " = " . $value;
                }
                break;
            case self::OP_IS_NULL :
                return " attribute_not_exists(" . $field . ")";
                break;
            case self::OP_IS_NOT_NULL :
                return " attribute_exists(" . $field . ")";
                break;
            default :
                return $field . " " . $op . " " . $value;
        }
    }

    /**
     * 按优先级拆分查询语句,嵌套的or操作只能filter处理
     * 
     * @param string $expression
     *            语句
     * @param string $cond
     *            and 或or
     * @return array 单个语句的数组组合
     */
    protected function explorCond($expression, $cond = self::COND_AND,$subIndex = -1)
    {
        $expression = trim ( $expression );
        if ($expression) {
            
            // TODO 有括号的放入filter，这种没有对保留字做处理
            if (preg_match ( "/^\(.*\)$/", $expression )) {
                $this->FilterExpression [] = $expression;
                return;
            }
            
            $strupper = strtoupper ( $expression );
            $opPreg = array (
                self::OP_EQ,
                self::OP_GT,
                self::OP_GTEQ,
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
                $this->explorCond ( $left, self::COND_AND );
                $right = substr ( $expression, $pos + 3 );
                $this->explorCond ( $right, self::COND_AND );
            } elseif (strpos ( $strupper, " " . self::COND_OR . " " ) > 0) {
                $pos = strpos ( $strupper, " " . self::COND_OR . " " );
                $left = substr ( $expression, 0, $pos );
                $this->explorCond ( $left, self::COND_OR );
                $right = substr ( $expression, $pos + 2 );
                $this->explorCond ( $right, self::COND_OR );
            } elseif (preg_match ( "/^(?<field>[\w\d_]+)\s*(?<op>" . $opPreg . ")\s*(?<val>:?[\w\d_]+)?$/i", $expression, $match )) {
                if ($cond == self::COND_OR) {
                    $this->orCond [] = array (
                        "Field" => $match ["field"],
                        "Op" => $match ["op"],
                        "Value" => isset ( $match ["val"] ) ? $match ["val"] : '' 
                    );
                } elseif ($cond == self::COND_AND) {
                    $this->andCond [] = array (
                        "Field" => $match ["field"],
                        "Op" => $match ["op"],
                        "Value" => isset ( $match ["val"] ) ? $match ["val"] : '' 
                    );
                }
                return;
            } else {
                // 放入filter
                if (! empty ( $this->FilterExpression )) {
                    $this->FilterExpression .= " " . $cond . " " . $expression;
                } else {
                    $this->FilterExpression = $expression;
                }
            }
        } else {
            throw new DbException ( "invalie condition" );
        }
    }

    /*
     * (non-PHPdoc) @see \Duomai\Db\Select\Select::checkCond()
     */
    protected function checkCond()
    {
        // describe table 获取表信息
        $result = $this->adapter->describeTable ( $this->tableName );
        
        // 所有属性名称
        foreach ( $result ["Table"] ["AttributeDefinitions"] as $attr ) {
            $this->tableInfo ["Fields"] [] = $attr ["AttributeName"];
        }
        // 主键名称
        foreach ( $result ["Table"] ["KeySchema"] as $keys ) {
            $this->tableInfo ["Keys"] [] = $keys ["AttributeName"];
        }
        // 全局索引名称
        if (isset ( $result ["Table"] ["GlobalSecondaryIndexes"] )) {
            foreach ( $result ["Table"] ["GlobalSecondaryIndexes"] as $keys ) {
                $indexName = $keys ["IndexName"];
                foreach ( $keys ["KeySchema"] as $key ) {
                    $this->tableInfo [$indexName] ["Indexs"] [] = $key ["AttributeName"];
                }
            }
        }
        // 本地索引名称
        if (isset ( $result ["Table"] ["LocalSecondaryIndexes"] )) {
            foreach ( $result ["Table"] ["LocalSecondaryIndexes"] as $keys ) {
                $indexName = $keys ["IndexName"];
                foreach ( $keys ["KeySchema"] as $key ) {
                    $this->tableInfo [$indexName] ["Indexs"] [] = $key ["AttributeName"];
                }
            }
        }
        // 2.检查ands，ors，columns，limit，order中的属性是否正确
        return $this->checkAnds () && $this->checkOrs () && $this->checkOrders ();
    }

    /**
     * 检查ands是否正确
     */
    protected function checkAnds()
    {
        // TODO
        return true;
    }

    protected function checkOrs()
    {
        // TODO
        return true;
    }

    protected function checkOrders()
    {
        // TODO
        return true;
    }
}