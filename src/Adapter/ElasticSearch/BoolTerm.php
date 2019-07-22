<?php
namespace Nosql\Adapter\ElasticSearch;
/**
 * bool查询
 *
 * @author roy
 *        
 */
class BoolTerm extends Term
{
    public $must = [ ];
    public $mustNot = [ ];
    public $should = [ ];

    /**
     * 添加must条件
     * @param Term $term
     */
    public function addMust($term)
    {
        $this->must [] = $term;
    }

    /**
     * 添加must_not条件
     * @param Term $term
     */
    public function addMustNot($term)
    {
        $this->mustNot [] = $term;
    }

    /**
     * 添加should条件
     * @param Term $term
     */
    public function addShould($term)
    {
        $this->should [] = $term;
    }

    /**
     * 转化为数组
     */
    public function toArray()
    {
        $result = [ ];
        if (! empty ( $this->must )) {
            foreach ( $this->must as $term ) {
                $result ["must"] [] = $term->toArray ();
            }
        }
        if (! empty ( $this->mustNot )) {
            foreach ( $this->mustNot as $term ) {
                $result ["must_not"] [] = $term->toArray ();
            }
        }
        if (! empty ( $this->should )) {
            foreach ( $this->should as $term ) {
                $result ["should"][] = $term->toArray ();
            }
            $result ["minimum_should_match"] = 1;
        }
        if(empty($result)){
            return ["bool"=>new \stdClass()];
        }
        return ["bool"=>$result];
    }
}