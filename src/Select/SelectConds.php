<?php
namespace Nosql\Select;

/**
 * 查询条件对象
 *
 * @author roy
 *        
 */
class SelectConds
{
    /**
     * 条件做用字段
     *
     * @var string
     */
    public $field;
    
    /**
     * 条件
     *
     * @var string Select::COND_AND Select::COND_OR
     */
    public $op;
    
    /**
     * 值，可以是字符串，数组
     *
     * @var mixed | array
     */
    public $value;
    
    /**
     * 下一级查询
     * 
     * @var SelectConds
     */
    public $childCond;
    
    /**
     * 与之同级的查询
     *
     * @var SelectConds
     */
    public $nextCond;
    
    /**
     * 与之同级的查询的条件
     *
     * @var string Select::COND_AND Select::COND_OR
     */
    public $nextCondType;

    /**
     * 设置条件
     * 
     * @param string $field
     *            字段
     * @param string $op
     *            条件
     * @param string $value
     *            值
     * @param string $nextCondType
     *            与下一个查询的条件
     */
    public function setCond($field, $op, $value, $nextCondType = null)
    {
        if (! empty ( $this->field )) {
            $cond = new self ();
            $cond->field = $field;
            $cond->op = $op;
            $cond->value = $value;
            $this->addCond ( $cond, $nextCondType );
        } else {
            $this->field = $field;
            $this->op = $op;
            $this->value = $value;
            $this->nextCondType = $nextCondType;
        }
    }

    /**
     * 添加下一个条件
     *
     * @param SelectConds $cond            
     * @param string $type            
     */
    public function addNextCond($cond, $type)
    {
        $lastCond = $this;
        while ( ! empty ( $lastCond->nextCond ) ) {
            $lastCond = $lastCond->nextCond;
        }
        if(empty($this->field)&&empty($this->childCond)){
            $this->setCond($cond->field, $cond->op, $cond->value,$type);
            $this->addChildCond($cond->childCond);
            $this->addNextCond($cond->nextCond, $cond->nextCondType);
        }else{
            $lastCond->nextCond = $cond;
            $lastCond->nextCondType = $type;
        }
    }

    /**
     * 添加子级条件
     * 
     * @param SelectConds $cond            
     */
    public function addChildCond($cond)
    {
        $this->childCond = $cond;
    }
}