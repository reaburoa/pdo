<?php
namespace Rean;

/**
 * PDOConnect only for connect to database through PDO
 */
class PDOConnect
{
    /**
     * the cluster which to find database servers
     */
    private static $cluster = null;

    /**
     * find database server in cluster
     */
    private static $hash = null;

    /**
     * origin conf
     */
    private static $origin_conf = [];

    /**
     * PDOConnect instance array
     */
    private static $instance = null;

    /**
     * PDO instance array
     */
    private static $pdo = null;

    /**
     * server's separator in cluster
     */
    private static $conf_separator = '|';

    /**
     * PDO default charset
     */
    private static $default_charset = 'utf8mb4';

    /**
     * PDO connect timeout default 5 seconds
     */
    private static $timeout = 5;

    /**
     * @param string $cluster cluster's name
     * @param mixed $hash hash value which can find database server
     * @throws \Exception when cluster is false
     * @return self
     */
    public static function getInstance(array $db_conf, $cluster, $hash = '')
    {
        if (empty($db_conf) || !$cluster) {
            throw new \Exception('Params cluster must be provide');
        }
        $key = $hash ? md5($cluster.substr($hash, 0, 1)) : md5($cluster);
        if (!self::$instance || !isset(self::$instance[$key])) {
            self::$instance[$key] = new self($db_conf, $cluster, $hash);
        }

        return self::$instance[$key];
    }

    /**
     * @param string $cluster 指定集群
     * @param mixed $hash 散列规则
     * @throws \Exception when cluster is false
     */
    public function __construct(array $db_conf, $cluster, $hash = '')
    {
        if (empty($db_conf) || !$cluster) {
            throw new \Exception('Params cluster must be provided');
        }
        self::$cluster = $cluster;
        self::$hash = $hash;
        self::$origin_conf = $db_conf;
    }

    /**
     * @return \PDO
     * @throws \Exception
     */
    public function getPDO()
    {
        $key = self::$hash ? md5(self::$cluster.substr(self::$hash, 0, 1)) : md5(self::$cluster);
        if (!self::$pdo || !isset(self::$pdo[$key])) {
            $conf = self::getConf(self::$origin_conf);
            if (!$conf || !is_array($conf)) {
                throw new \Exception('The conf is error');
            }
            $option = self::getOption();
            try {
                $pdo = new \PDO($conf['dsn'], $conf['user'], $conf['password'], $option);
                self::$pdo[$key] = $pdo;
                return $pdo;
            } catch(\PDOException $e) {
                throw new \Exception($e->getMessage(), $e->getCode());
            }
        } else {
            return self::$pdo[$key];
        }
    }

    /**
     * get database conf
     */
    private static function getConf($db_conf)
    {
        if (empty($db_conf)) {
            throw new \Exception("Cannot Get Database Conf");
        }
        if (!isset($db_conf[self::$cluster])) {
            throw new \Exception("Conf has no ".self::$cluster." cluster");
        }
        if (strpos($db_conf[self::$cluster], self::$conf_separator) === false) {
            $ret = $db_conf[$db_conf[self::$cluster]];
        } else {
            $hash = self::hashCluster($db_conf[self::$cluster], self::$hash);
            $ret = $db_conf[$hash];
        }
        return $ret;
    }

    /**
     * get database server detail info
     * @param string $cluster
     * @param string $hash
     * @return string
     */
    private static function hashCluster($cluster, $hash)
    {
        $node = explode(self::$conf_separator, $cluster);
        $node_num = count($node);
        $hash_num = hexdec(substr($hash, 0, 1));
        $section = ceil(16 / $node_num);
        $reverse_section = array_reverse(range($node_num * $section - 1, 0, -$section));
        $cluster_num = 0;
        foreach ($reverse_section as $key => $value) {
            if ($key == 0 && $hash_num <= $value) {
                $cluster_num = 0;
                break;
            } elseif ($hash_num <= $reverse_section[$key] && $hash_num > $reverse_section[$key - 1]) {
                $cluster_num = $key;
                break;
            }
        }
        return $node[$cluster_num];
    }

    /**
     * the options when connect to PDO
     */
    private static function getOption()
    {
        return [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES '".self::$default_charset."'",
            \PDO::ATTR_TIMEOUT => self::$timeout,
        ];
    }
}