<?php
define('ROOT_NAMESPACE', 'Rean');
define('ROOT_PATH', __DIR__);
define('CONF_PATH', ROOT_PATH.'/conf/');

spl_autoload_register(function ($class) {
    $src_dir = ROOT_PATH.'/src/';
    $ar_namespace = explode('\\', $class);
    if (count($ar_namespace) == 1 || $ar_namespace[0] != ROOT_NAMESPACE) {
        return;
    }
    unset($ar_namespace[0]);
    $str_path = implode('/', $ar_namespace);
    $file = $src_dir.$str_path.'.php';
    if (file_exists($file)) {
        require $file;
    }
});
