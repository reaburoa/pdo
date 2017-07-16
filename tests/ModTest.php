<?php
include_once '../vendor/autoload.php';

use Rean\MyPDO;

class ModTest extends MyPDO
{
    protected static $database = 'd_test_?';
    protected static $table = 't_test_???';
    protected static $hash_key = null;
    protected static $cluster = 'i_test';
    protected static $data_format = [
        'id' => self::FIELD_INT,
        'name' => self::FIELD_STRING,
        'phone' => self::FIELD_STRING,
        'age' => self::FIELD_INT,
        'age_1' => self::FIELD_INT
    ];

    public function insert(array $params, $last_insert_id = false)
    {
        self::$hash_key = 'a';
        return parent::insert($params);
    }

    public function getAll(array $cond = [], array $order = [], $offset = 0, $limit = 0, array $field = [])
    {
        self::$hash_key = 'a';
        return parent::getAll($cond, $order, $offset, $limit, $field);
    }

    public function getOneRow(array $cond = [], array $order = [], array $field = [])
    {
        self::$hash_key = 'a';
        return parent::getOneRow($cond, $order);
    }

    public function update(array $cond = [], array $values = [], array $order = [], $limit = 0)
    {self::$hash_key = 'a';
        return parent::update($cond, $values, $order, $limit);
    }

    public function delete(array $cond = [], array $order = [], $limit = 0)
    {self::$hash_key = 'a';
        return parent::delete($cond, $order, $limit);
    }

    public function getOne($field, array $cond = [], array $order = [])
    {
        self::$hash_key = 'a';
        return parent::getOne($field, $cond, $order);
    }

    public function sum($field, array $cond = [])
    {
        self::$hash_key = 'a';
        return parent::sum($field, $cond);
    }

    public function count(array $cond = [])
    {
        self::$hash_key = 'a';
        return parent::count($cond);
    }

    public function increment(array $increment, array $cond = [])
    {
        self::$hash_key = 'a';
        return parent::increment($increment, $cond);
    }

    public function decrement(array $increment, array $cond = [])
    {
        self::$hash_key = 'a';
        return parent::decrement($increment, $cond);
    }

    public function query($sql, array $params = [])
    {
        self::$hash_key = 'a';
        $sql = "SELECT * FROM ".$this->getDB()." WHERE id=:id";
        return parent::query($sql, $params);
    }

    public function exec($sql, array $params = [])
    {
        self::$hash_key = 'a';
        return parent::exec($sql, $params);
    }
}

$mod = new ModTest();
#$ret = $mod->insert(['phone' => rand(100000, 1000000), 'name' => '姓名'.rand(88, 10000), 'age' => rand(1, 99)]);
#$ret = $mod->update(['name' => 'test', 'age' => 19], ['name' => 'test_zhanglei', 'age' => 29]);
$page = $_GET['page'];
$limit = 20;
$offset = ($page - 1) * $limit;
$ret = $mod->getAll(['or' => ['name' => ['like' => '%test%'], 'id' => ['in' => [56, 57]]]], ['id' => 'desc'], $offset, $limit);
#$ret = $mod->getOne('name', ['name' => ['like' => '%test%'], 'or' => ['id' => '5', 'name' => 'test_zhanglei']], ['id' => 'asc']);
#$ret = $mod->getOneRow(['name' => ['like' => '姓_'], 'id' => ['in' => [12]]]);
#$ret = $mod->increment(['age' => 1, 'age_1' => 3], ['id' => 6]);
#$ret = $mod->query('', [':id' => 12]);
#$ret = $mod->exec("update d_test_0.t_test_0cc set age=:age where id=:id", [':age' => 19, ':id' => 5]);
echo '<pre>';
print_r($ret);
echo '</pre>';