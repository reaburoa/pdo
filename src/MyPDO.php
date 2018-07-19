<?php
namespace Rean;

/**
 * PDO library
 */
class MyPDO
{
    /**
     * model will operate database
     */
    protected static $database = null;

    /**
     * model will operate table
     */
    protected static $table = null;

    /**
     * the cluster where database in
     */
    protected static $cluster = null;

    /**
     * database hash rule
     */
    protected static $hash_key = null;

    /**
     * data format for return format data
     */
    protected static $data_format = null;

    const FIELD_INT = 'int';
    const FIELD_STRING = 'string';
    const FIELD_DOUBLE = 'double';

    const DEFAULT_SELECT_FIELD = '*';

    const DEFAULT_STRING_ENCODE = 'utf8';

    protected static $field_2_pdo_param = [
        self::FIELD_INT => \PDO::PARAM_INT,
        self::FIELD_STRING => \PDO::PARAM_STR
    ];

    private static $ar_operate = [
        'or',
        '>',
        '>=',
        '<',
        '<=',
        '!=',
        'in',
        'not in',
        'like',
        'not like'
    ];
    const AND_OP = 'AND';
    const OR_OP = 'OR';

    public function __construct()
    {
    }

    private function getHash()
    {
        return static::$hash_key ? md5(static::$hash_key) : '';
    }

    private function getCluster()
    {
        return static::$cluster;
    }

    protected function getDB()
    {
        if (strpos(static::$database, '?') === false) {
            $database = static::$database;
            $table = static::$table;
        } else {
            $db_count = substr_count(static::$database, '?');
            $tb_count = substr_count(static::$table, '?');
            $hash = $this->getHash();
            $database = substr_replace(static::$database, substr($hash, 0, $db_count), strlen(static::$database) - $db_count);
            $table = substr_replace(static::$table, substr($hash, 0, $tb_count), strlen(static::$table) - $tb_count);
        }
        return '`'.$database.'`.`'.$table.'`';
    }

    /**
     * @return \PDO
     */
    private function getPDO()
    {
        return PDOConnect::getInstance($this->getCluster(), $this->getHash())->getPDO();
    }

    private function format(array $value)
    {
        if (!self::$data_format || !is_array(self::$data_format)) {
            return $value;
        }
        foreach (static::$data_format as $key => $val) {
            if (!isset($value[$key])) {
                continue;
            }
            switch($val) {
                case self::FIELD_STRING:
                    $value[$key] = (string)$value[$key];
                    break;
                case self::FIELD_INT:
                    $value[$key] = (int)$value[$key];
                    break;
                case self::FIELD_DOUBLE:
                    $value[$key] = (double)$value[$key];
                    break;
            }
        }

        return $value;
    }

    protected function insert(array $param_values)
    {
        if (empty($param_values)) {
            return false;
        }

        $sql = "INSERT INTO ".$this->getDB();
        $bind_params = [];
        foreach ($param_values as $key => $value) {
            $bind_params[':'.$key] = $value;
        }
        $sql .= "(`".implode("`,`", array_keys($param_values))."`) VALUES (".implode(",", array_keys($bind_params)).")";
        return $this->execute($sql, $bind_params, 'insert');
    }

    protected function update(array $cond, array $values, array $order = [], $limit = 0)
    {
        $update_keys = [];
        $update_values = [];
        $values_i = 0;
        foreach ($values as $key => $value) {
            $values_i ++;
            $tmp_key = ':'.$values_i;
            $update_keys[] = '`'.$key.'`='.$tmp_key;
            $update_values[$tmp_key] = $value;
        }
        $update_string = implode(',', $update_keys);
        if (!$update_string) {
            return false;
        }
        $sql = "UPDATE ".$this->getDB()." SET ".$update_string;
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && is_array($format_cond)) {
            $params_keys = [];
            foreach ($format_cond['where_values'] as $k => $val) {
                $values_i ++;
                $tmp_key = ':'.$values_i;
                $params_keys[$k] = $tmp_key;
                $update_values[$tmp_key] = $val;
            }
            if ($params_keys) {
                $sql .= " WHERE ".str_replace(array_keys($params_keys), array_values($params_keys), $format_cond['where_sql']);
            }
        }
        $format_order = $this->formatOrder($order);
        if ($format_order) {
            $sql .= $format_order;
        }
        if ((int)$limit > 0) {
            $sql .= ' LIMIT '.(int)$limit;
        }
        $ret = $this->execute($sql, $update_values, 'exec');
        return $ret;
    }

    protected function delete(array $cond = [], array $order = [], $limit = 0)
    {
        $sql = "DELETE FROM ".$this->getDB();
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && isset($format_cond['where_sql']) && $format_cond['where_sql']) {
            $sql .= " WHERE ".$format_cond['where_sql'];
        }
        $format_order = $this->formatOrder($order);
        if ($format_order) {
            $sql .= $format_order;
        }
        if ((int)$limit > 0) {
            $sql .= " LIMIT ".(int)$limit;
        }
        $params = $format_cond && isset($format_cond['where_values']) ? $format_cond['where_values'] : [];
        $ret = $this->execute($sql, $params, 'exec');
        return $ret;
    }

    protected function getAll(array $cond = [], array $order = [], $offset = 0, $limit = 0, array $field = [])
    {
        $format_field = $this->formatField($field);
        $sql = "SELECT ".$format_field." FROM ".$this->getDB();
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && isset($format_cond['where_sql']) && $format_cond['where_sql']) {
            $sql .= " WHERE ".$format_cond['where_sql'];
        }
        $format_order = $this->formatOrder($order);
        if ($format_order) {
            $sql .= $format_order;
        }
        if ((int)$limit > 0) {
            $sql .= " LIMIT ".((int)$offset >= 0 ? (int)$offset : 0).",".(int)$limit;
        }
        $params = $format_cond && isset($format_cond['where_values']) ? $format_cond['where_values'] : [];
        $ret = $this->execute($sql, $params, 'fetchAll');
        return $ret;
    }

    protected function getOneRow(array $cond = [], array $order = [], array $field = [])
    {
        $format_field = $this->formatField($field);
        $sql = "SELECT ".$format_field." FROM ".$this->getDB();
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && isset($format_cond['where_sql']) && $format_cond['where_sql']) {
            $sql .= " WHERE ".$format_cond['where_sql'];
        }
        $format_order = $this->formatOrder($order);
        if ($format_order) {
            $sql .= $format_order;
        }
        $params = $format_cond && isset($format_cond['where_values']) ? $format_cond['where_values'] : [];
        $ret = $this->execute($sql, $params, 'fetch');
        return $ret;
    }

    protected function getOne($field, array $cond = [], array $order = [])
    {
        if (!$field) {
            return false;
        }
        $sql = "SELECT `{$field}` FROM ".$this->getDB();
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && isset($format_cond['where_sql']) && $format_cond['where_sql']) {
            $sql .= " WHERE ".$format_cond['where_sql'];
        }
        $format_order = $this->formatOrder($order);
        if ($format_order) {
            $sql .= $format_order;
        }
        $params = $format_cond && isset($format_cond['where_values']) ? $format_cond['where_values'] : [];
        $ret = $this->execute($sql, $params, 'fetchColumn');
        return $ret;
    }

    protected function count(array $cond = [])
    {
        $sql = "SELECT COUNT(*) FROM ".$this->getDB();
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && isset($format_cond['where_sql']) && $format_cond['where_sql']) {
            $sql .= " WHERE ".$format_cond['where_sql'];
        }
        $params = $format_cond && isset($format_cond['where_values']) ? $format_cond['where_values'] : [];
        $ret = $this->execute($sql, $params, 'fetchColumn');
        return $ret;
    }

    protected function sum($field, array $cond = [])
    {
        if (!$field) {
            return false;
        }
        $sql = "SELECT SUM({$field}) FROM ".$this->getDB();
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && isset($format_cond['where_sql']) && $format_cond['where_sql']) {
            $sql .= " WHERE ".$format_cond['where_sql'];
        }
        $params = $format_cond && isset($format_cond['where_values']) ? $format_cond['where_values'] : [];
        $ret = $this->execute($sql, $params, 'fetchColumn');
        return $ret;
    }

    protected function increment(array $increment, array $cond = [])
    {
        if (empty($increment)) {
            return false;
        }
        $tmp_increment = [];
        foreach ($increment as $key => $value) {
            $tmp_increment[] = "`{$key}`=`{$key}`+".(int)$value;
        }
        $sql = "UPDATE ".$this->getDB()." SET ".implode(',', $tmp_increment);
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && isset($format_cond['where_sql']) && $format_cond['where_sql']) {
            $sql .= " WHERE ".$format_cond['where_sql'];
        }
        $params = $format_cond && isset($format_cond['where_values']) ? $format_cond['where_values'] : [];
        $ret = $this->execute($sql, $params, 'exec');
        return $ret;
    }

    protected function decrement(array $increment, array $cond = [])
    {
        if (empty($increment)) {
            return false;
        }
        $tmp_increment = [];
        foreach ($increment as $key => $value) {
            $tmp_increment[] = "`{$key}`=`{$key}`-".(int)$value;
        }
        $sql = "UPDATE ".$this->getDB()." SET ".implode(',', $tmp_increment);
        $format_cond = $this->formatCondition($cond);
        if ($format_cond && isset($format_cond['where_sql']) && $format_cond['where_sql']) {
            $sql .= " WHERE ".$format_cond['where_sql'];
        }
        $params = $format_cond && isset($format_cond['where_values']) ? $format_cond['where_values'] : [];
        $ret = $this->execute($sql, $params, 'exec');
        return $ret;
    }

    protected function query($sql, array $params = [])
    {
        if (!$sql) {
            return false;
        }
        $ret = $this->execute($sql, $params, 'fetchAll');
        return $ret;
    }

    protected function exec($sql, array $params = [])
    {
        if (!$sql) {
            return false;
        }
        $ret = $this->execute($sql, $params, 'exec');
        return $ret;
    }

    private function execute($sql, array $params, $operator)
    {
        if (!$sql || !$operator) {
            return false;
        }
        try {
            $pdo = $this->getPDO();
            $pdo_statement = $pdo->prepare($sql);
            if ($params) {
                foreach ($params as $key => $val) {
                    $cond_key = substr($key, 1);
                    $ar_key = strpos($cond_key, '_') !== false ? explode('_', $cond_key) : $cond_key;
                    if (is_array($ar_key) && is_numeric($ar_key[count($ar_key) - 1])) {
                        unset($ar_key[count($ar_key) - 1]);
                        $cond_key = implode('_', $ar_key);
                    }
                    $data_type = '';
                    if (!is_numeric($cond_key) && isset(static::$field_2_pdo_param[static::$data_format[$cond_key]])) {
                        $data_type = static::$field_2_pdo_param[static::$data_format[$cond_key]];
                    }
                    $data_type ? $pdo_statement->bindValue($key, $val, $data_type) : $pdo_statement->bindValue($key, $val);
                }
            }
            $exec_ret = $pdo_statement->execute();
            if ($exec_ret === false) {
                return false;
            }
            switch ($operator) {
                case 'insert':
                    $last_insert_id = $pdo->lastInsertId();
                    $result = $exec_ret && $last_insert_id > 0 ? $last_insert_id : $exec_ret;
                    break;
                case 'exec':
                    $affect_rows = $pdo_statement->rowCount();
                    $result = $affect_rows > 0 ? $affect_rows : $exec_ret;
                    break;
                case 'fetchColumn':
                    $result = $pdo_statement->fetchColumn();
                    break;
                case 'fetch':
                    $ret = $pdo_statement->fetch(\PDO::FETCH_ASSOC);
                    $error_code = $pdo->errorCode();
                    $result = !$ret && $error_code == '00000' ? [] : $this->format($ret);
                    break;
                case 'fetchAll':
                    $result = $pdo_statement->fetchAll(\PDO::FETCH_ASSOC);
                    foreach ($result as &$value) {
                        $value = $this->format($value);
                    }
                    break;
                default:
                    return false;
                    break;
            }
            return $result;
        } catch (\PDOException $e) {
            $error_info = [
                'code' => $e->getCode(),
                'msg' => $e->getMessage()
            ];
            throw new \Exception(json_encode($error_info));
        }
    }

    protected function formatWhere(array $condition)
    {
        if (empty($condition)) {
            return false;
        }
        $flip_operate = array_flip(self::$ar_operate);
        $ar_sql = [];
        $ar_values = [];
        $same_key_i = 0;
        foreach ($condition as $key => $value) {
            if (strtoupper($key) == self::OR_OP) {
                if (!is_array($value) || empty($value)) {
                    continue;
                }
                $result = $this->formatWhere($value);
                foreach ($result['bind_where_sql'][self::AND_OP] as $k => $v) {
                    $ar_sql[self::OR_OP][$k] = $v;
                }
                foreach ($result['bind_where_values'] as $k => $v) {
                    $new_k = $k;
                    if (isset($ar_values[$k])) {
                        $same_key_i ++;
                        $new_k = $k.'_'.$same_key_i;
                    }
                    $ar_sql[self::OR_OP] = array_map(function($value) use ($k, $new_k) {
                        return str_replace($k, $new_k, $value);
                    }, $ar_sql[self::OR_OP]);
                    $ar_values[$new_k] = $v;
                }
                continue;
            }
            $bind_key = ':'.$key;
            if (!is_array($value)) {
                $ar_sql[self::AND_OP][] = "`{$key}`={$bind_key}";
                $ar_values[$bind_key] = $value;
                continue;
            }
            foreach ($value as $ke => $val) {
                if (!isset($flip_operate[$ke])) {
                    continue;
                }
                if (isset($ar_values[$bind_key])) {
                    $same_key_i ++;
                    $bind_key .= '_'.$same_key_i;
                }
                switch ($ke) {
                    case 'in':
                    case 'not in':
                        if (!is_array($val) || empty($val)) {
                            continue;
                        }
                        $in_i = 0;
                        $in_params = [];
                        foreach ($val as $va) {
                            $in_i ++;
                            $in_bind_key = ":".$same_key_i.$in_i;
                            $in_params[$in_bind_key] = $va;
                            $ar_values[$in_bind_key] = $va;
                        }
                        $ar_sql[self::AND_OP][] = "`{$key}` ".strtoupper($ke)." (".implode(',', array_keys($in_params)).")";
                        break;
                    case 'like':
                    case 'not like':
                        if (is_array($val) || empty($val)) {
                            continue;
                        }
                        $ar_sql[self::AND_OP][] = "`{$key}` ".strtoupper($ke).' '.$bind_key;
                        $ar_values[$bind_key] = $val;
                        break;
                    default:
                        if (is_array($val) || empty($val)) {
                            continue;
                        }
                        $ar_values[$bind_key] = $val;
                        $ar_sql[self::AND_OP][] = "`{$key}` {$ke} {$bind_key}";
                        break;
                }
            }
        }
        return [
            'bind_where_sql' => $ar_sql,
            'bind_where_values' => $ar_values
        ];
    }

    /**
     * this PDO Class support follow operator and some example
     * >, <, =, !=, >=, <=, in, like, not in, not like, or,
     * caller transmit                      params                       bind_params
     * ['id' => 1, 'phone' => '1234']       `id`=:id                     [':id' => 1, ':phone' => 1234]
     * ['id' => ['>' => 1, '<' => 3]]       `id`>:id_1 and 'id'<:id_2    [':id_1' => 1, ':id_2' => 3]
     * ['id' => ['in' => [1, 2, 3]]]        `id` in :id                  [':id' => [1, 2, 3]]
     * ['id' => ['!=' => 4]]                `id`!=:id                    [':id' => 4]
     * ['phone' => ['like' => '%12%']]      `phone` like :phone          [':phone' => '%12%']
     * ['phone' => ['not like' => '%12%']]  `phone` not like :phone      [':phone' => '%12%']
     * ['id' => ['not in' => [5, 6, 7]]]    `id` not in :id              [':id' => [5, 6, 7]]
     * ['or' => ['id' => 1, 'name' => 2]]   `id`=:id OR `name`=:name     [':id' => 1, ':name' => 2]
     * ['or' => ['name' => ['like' => '%test%'], 'id' => ['in' => [56]]]]   `name` like :name or id in (:1) [':name' => '%test%', ':1' => 56]
     * ['phone' => 1, 'or' => ['name' => 'test','id' => 56]]    `phone`=:phone AND (`name`=:name OR `id`=:id)   [':phone' => 1,':name' => test,':id' => 56]
     *
     * @param array $cond the condition of caller transmit
     * @return mixed array will be returned,False is returned when $cond is empty
     */
    private function formatCondition(array $cond)
    {
        if (empty($cond)) {
            return false;
        }
        $ret = $this->formatWhere($cond);
        if ($ret === false) {
            return false;
        }
        $str_sql = isset($ret['bind_where_sql'][self::AND_OP]) ? implode(' '.self::AND_OP.' ', $ret['bind_where_sql'][self::AND_OP]) : '';
        if (isset($ret['bind_where_sql'][self::OR_OP])) {
            $or_sql = implode(" ".self::OR_OP." ", $ret['bind_where_sql'][self::OR_OP]);
            $str_sql .= $str_sql ? " ".self::AND_OP." ({$or_sql})" : $or_sql;
        }
        return [
            'where_sql' => $str_sql,
            'where_values' => $ret['bind_where_values']
        ];
    }

    private function formatField(array $field)
    {
        return $field ? '`'.implode('`,`', $field).'`' : self::DEFAULT_SELECT_FIELD;
    }

    private function formatOrder(array $order)
    {
        if (empty($order)) {
            return false;
        }
        $str_order = ' ORDER BY ';
        foreach ($order as $key => $value) {
            $str_order .= $key." ".strtoupper($value).",";
        }
        return substr($str_order, 0, -1);
    }
}
