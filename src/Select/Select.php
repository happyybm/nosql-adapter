<?php
namespace Nosql\Select;

use Nosql\DbException;
use Nosql\Select\SelectConds;
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
     * 查询条件
     * @var SelectConds
     */
    protected  $selectConds=null;
    
    
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
        $this->selectConds = new SelectConds();
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
     *            或：Field=? And Field!=? 其中?是条件占位符，在参数bind中指定其值。
     * @param array $bind
     *            绑定的参数,由cond中的参数占位符指定
     *            例：array("p1"=>"val1","p2"=>"val2")
     *            或：array("val1","val2")
     * @return Select
     */
    public function where($cond, $bind=null)
    {
    	//?号依次替换
    	if(preg_match("/\?/", $cond)){
    		if(!is_array($bind)){
    			$bind=[$bind];
    		}
    		foreach ($bind as $p){
	    		$cond = preg_replace("/\?/", $this->adapter->quoteStr($p), $cond,1);
    		}
    	}else if(!empty($bind)&&is_array($bind)){
            $this->binds = array_merge ( $this->binds, $bind );
        }
        $this->ands [] = $cond;
        return $this;
    }

    /**
     * 查询条件 or查询，
     * 
      * @param string $cond
     *            查询表达式
     *            例：Field=:p1: And Field!=:p2: 其中p1，跟p2就是条件占位符，在参数bind中指定其值。
     *            或：Field=? And Field!=? 其中?是条件占位符，在参数bind中指定其值。
     * @param array $bind
     *            绑定的参数,由cond中的参数占位符指定
     *            例：array("p1"=>"val1","p2"=>"val2")
     *            或：array("val1","val2")
     * @return Select
     */
    public function orWhere($cond, $bind)
    {
    	//?号依次替换
    	if(preg_match("/\?/", $cond)){
    		if(!is_array($bind)){
    			$bind=[$bind];
    		}
    		foreach ($bind as $p){
	    		$cond = preg_replace("/\?/", $this->adapter->quoteStr($p), $cond,1);
    		}
    	}else if(!empty($bind)&&is_array($bind)){
    		$this->binds = array_merge ( $this->binds, $bind );
    	}
    	$this->ors [] = $cond;
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
     * @param string $expression 当前级查询条件
     * @param string $type and 或or 父级与当前级的条件
     * @param SelectConds $parentCond 父级查询条件
     * @return SelectConds 生成新的查询条件
     */
    protected function explorCond($expression, $type = self::COND_AND)
    {
        $expression = trim ( $expression );
        if ($expression) {
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
            // 按优先级，先按()拆分，再按and拆分
            if (preg_match ( "/^\((?<subExp>.*)\)\s*$/i", $expression, $match )) {
            	//1.(a=b or b=c)
            	//2.(a=b or b=c and (c=c or d=d)) and c=c
            	//3. a=a and b=b and (c=c or d=d or (a=a and b=b))
            	return  $this->explorCond ( $match ["subExp"], $type );
            	
            }else if (strpos ( $strupper, " " . self::COND_AND . " " ) > 0) {
                $pos = strpos ( $strupper, " " . self::COND_AND . " " );
                $left = substr ( $expression, 0, $pos );
                $leftCond = $this->explorCond ( $left, self::COND_AND );
                
                $right = substr ( $expression, $pos + 4 );
                $rightCond = $this->explorCond ( $right, self::COND_AND );
                if($leftCond&&$rightCond){
                	$leftCond->addNextCond($rightCond, $type);
                    return $leftCond;
                }else {
                    return $leftCond?$leftCond:$rightCond;
                }
            } elseif (strpos ( $strupper, " " . self::COND_OR . " " ) > 0) {
                $pos = strpos ( $strupper, " " . self::COND_OR . " " );
                $left = substr ( $expression, 0, $pos );
                $leftCond = $this->explorCond ( $left, self::COND_OR );
                
                $right = substr ( $expression, $pos + 3 );
                $rightCond = $this->explorCond ( $right, self::COND_OR );
                if($leftCond&&$rightCond){
                	$leftCond->addNextCond($rightCond, $type);
                    return $leftCond;
                }else {
                    return $leftCond?$leftCond:$rightCond;
                }
            } elseif (preg_match ( "/^(?<field>[\w\d_]+)\s*(?<op>" . $opPreg . ")\s*(?<val>:?[\w\d_]+:?)?$/i", $expression, $match )) {
                $value = isset ( $match ["val"] ) ? trim($match ["val"],":") : '';
                $leaveCond = new SelectConds();
                $leaveCond->setCond($match ["field"], $match ["op"],$value);
                return $leaveCond;
            } else {
                //不支持的查询条件
                throw new DbException ( "unsupport condition：".$expression );
            }
        }else{
            return null;
        }
    }

}