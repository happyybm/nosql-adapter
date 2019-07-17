<?php
namespace Nosql\Adapter;
/**
 * 结果集
 *
 * @author roy
 */
class Rowset implements \SeekableIterator, \Countable
{
    /**
     * 结果数组
     * 
     * @var array
     */
    protected $data;
    /**
     * 结果总计
     * 
     * @var int
     */
    protected $total;
    
    protected $count;
    /**
     * 结果数组索引
     * 
     * @var integer
     */
    protected $pointer = 0;

    public function __construct($data, $count)
    {
        $this->data = $data;
        $this->total = $count;
        $this->count = count($this->data);
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see Countable::count()
     */
    public function count()
    {
        return $this->count;
    }
    
    /**
     * 返回总数
     * @return number
     */
    public function getTotal(){
        return $this->total;
    }
    
    /**
     * 返回结果数组
     * @return array
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see SeekableIterator::seek()
     */
    public function seek($position)
    {
        $this->pointer = $position;
        return $this;
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see SeekableIterator::current()
     */
    public function current()
    {
        return $this->data [$this->pointer];
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see SeekableIterator::next()
     */
    public function next()
    {
        $this->pointer ++;
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see SeekableIterator::key()
     */
    public function key()
    {
        return $this->pointer;
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see SeekableIterator::valid()
     */
    public function valid()
    {
        return $this->pointer >= 0 && $this->pointer < $this->count;
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see SeekableIterator::rewind()
     */
    public function rewind()
    {
        $this->pointer = 0;
        return $this;
    }
}