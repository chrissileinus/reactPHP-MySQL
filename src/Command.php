<?php
/*
 * Created on Wed Feb 23 2022
 *
 * Copyright (c) 2022 Christian Backus (Chrissileinus)
 *
 * For the full copyright and license information, please view the LICENSE file that was distributed with this source code.
 */

namespace Chrissileinus\React\MySQL;

/**
 * A pool of React\MySQL connections with additional commandset like Query, Insert.....
 */
class Command extends Pool
{
  /**
   * Prepare a insert statment and performs an async query.
   * 
   * This method returns a promise that will resolve with a `QueryResult` on
   * success or will reject with an `Exception` on error. 
   *
   * @param  string                          $table   Table name
   * @param  array                           $inserts A array with associative arrays with matching keys and values
   * @param  array|null                      $indexes A array of indexes to perform "ON DUPLICATE KEY UPDATE"
   * @return \React\Promise\PromiseInterface
   */
  static function insert(string $table, array $inserts, array $indexes = null): \React\Promise\PromiseInterface
  {
    if (array_depth($inserts) < 2) $inserts = [$inserts];

    $fields = (function () use ($inserts) {
      $t = [];
      foreach (array_keys($inserts[0]) as $key => $field) {
        $t[] = "`{$field}`";
      }
      return implode(", ", $t);
    })();

    $values = (function () use ($inserts) {
      $t = [];
      foreach ($inserts as $entry) {
        $tt = [];
        foreach (array_values($entry) as $key => $value) {
          $tt[] = quote($value);
        }
        $tt = implode(", ", $tt);

        $t[] = "( {$tt} )";
      }
      return implode(",\n ", $t);
    })();

    $updates = (function () use ($inserts, $indexes) {
      if (!$indexes || !count($indexes)) return "";

      $t = [];
      foreach (array_keys($inserts[0]) as $field) {
        if (array_search($field, $indexes) === false) {
          $t[] = "`{$field}` = new.{$field}";
        }
      }
      return "\n AS new ON DUPLICATE KEY UPDATE\n " . implode(",\n ", $t);
    })();

    $query = "INSERT\n INTO `{$table}`\n (\n {$fields}\n )\n VALUES\n {$values}{$updates};";

    // \Chrissileinus\React\Log\Writer::debug("SQL:" . PHP_EOL . $query, 'DataBase');
    return self::query($query);
  }

  /**
   * Prepare a update statment and performs an async query.
   * 
   * This method returns a promise that will resolve with a `QueryResult` on
   * success or will reject with an `Exception` on error. 
   *
   * @param  string                          $table Table name
   * @param  array                           $set   A associative arrays with matching keys and values
   * @param  array                           $where
   * @param  int|array                       $limit A int as limit or a array like [int offset, int limit]
   * @return \React\Promise\PromiseInterface
   */
  static function update(string $table, array $set, array $where, $limit = null): \React\Promise\PromiseInterface
  {
    $sqlWhere = self::genWhere($where);
    $sqlLimit = self::genLimit($limit);

    $sqlSets = (function () use ($set) {
      $t = [];
      foreach ($set as $key => $value) {
        if (preg_match('/\w+\(\)$/', $value, $_)) {
          $t[] = "`{$key}` = " . $value;
          continue;
        }

        $t[] = "`{$key}` = " . quote($value);
      }
      return implode(",\n  ", $t);
    })();

    $query = "UPDATE `{$table}`\n SET\n  {$sqlSets}{$sqlWhere}{$sqlLimit};";

    // \Chrissileinus\React\Log\Writer::debug("SQL:" . PHP_EOL . $query, 'DataBase');
    return self::query($query);
  }

  /**
   * Prepare a delete statment and performs an async query.
   * 
   * This method returns a promise that will resolve with a `QueryResult` on
   * success or will reject with an `Exception` on error. 
   *
   * @param  string                          $table Table name
   * @param  array                           $where 
   * @param  int|array                       $limit A int as limit or a array like [int offset, int limit]
   * @return \React\Promise\PromiseInterface
   */
  static function delete(string $table, array $where, $limit = null): \React\Promise\PromiseInterface
  {
    $sqlWhere = self::genWhere($where);
    $sqlLimit = self::genLimit($limit);

    $query = "DELETE\n FROM `{$table}`{$sqlWhere}{$sqlLimit};";

    // \Chrissileinus\React\Log\Writer::debug("SQL:" . PHP_EOL . $query, 'DataBase');
    return self::query($query);
  }

  /**
   * Prepare a select statment and performs an async query.
   * 
   * This method returns a promise that will resolve with a `QueryResult` on
   * success or will reject with an `Exception` on error. 
   *
   * @param  string                          $table  Table name
   * @param  string|array                    $fields A array of selected Fields or "*"
   * @param  array|null                      $where
   * @param  string|array                    $order
   * @param  int|array                       $limit  A int as limit or a array like [int offset, int limit]
   * @return \React\Promise\PromiseInterface
   */
  static function select(string $table, $fields = null, array $where = null, $order = null, $limit = null): \React\Promise\PromiseInterface
  {
    $sqlFields = (function () use ($fields) {
      if (is_string($fields)) return "`{$fields}`";
      if (is_array($fields)) return implode(", ", array_map(function ($field) {
        return "`{$field}`";
      }, $fields));
      return "*";
    })();

    $sqlWhere = self::genWhere($where);

    $sqlOrder = (function () use ($order) {
      $assembleEntry = function ($order) {
        if (preg_match("/^(?<field>[^\[\]]+)\[(?<direction>.+)\]$/", $order, $match)) {
          extract($match);
          $direction = isset($direction) ? $direction : "";
          return "`{$field}` {$direction}";
        }
        return "`{$order}`";
      };

      $pre = "\n ORDER BY ";
      if (is_string($order)) return $pre . $assembleEntry($order);
      if (is_array($order)) return $pre . implode(", ", array_map($assembleEntry, $order));
      return "";
    })();

    $sqlLimit = self::genLimit($limit);

    $query = "SELECT {$sqlFields}\n FROM `{$table}`{$sqlWhere}{$sqlOrder}{$sqlLimit};";

    // \Chrissileinus\React\Log\Writer::debug("SQL:" . PHP_EOL . $query, 'DataBase');
    return self::query($query);
  }

  static protected function genWhere($where, bool $noPrefix = false): string
  {
    if (!is_array($where)) return "";
    $where = array_filter($where, 'is_string', ARRAY_FILTER_USE_KEY);
    if (!$where) return "";

    $assembleEntry = function ($key, $value) {
      if (preg_match("/^(?<name>[^\[\]]+)(?:\[(?<condition>.+)\]){0,1}$/", $key, $match)) {
        extract($match);
        $condition = isset($condition) ? $condition : "=";
        return " `{$name}` {$condition} " . quote($value);
      }
    };

    $condition = 'AND';
    $firstKey = array_key_first($where);
    if (
      $firstKey == 'AND' ||
      $firstKey == 'OR'
    ) {
      $condition = $firstKey;
      $where = array_filter($where[$condition], 'is_string', ARRAY_FILTER_USE_KEY);
    }
    if (!$where) return "";

    $return = $noPrefix ? "" : "\n WHERE";
    if (count($where) == 1) {
      $key = array_key_first($where);
      return $return . $assembleEntry($key, $where[$key]);
    }

    $return .= "\n  (";
    $first = true;
    foreach ($where as $key => $value) {
      if (!$first) $return .= " {$condition}";

      if (is_array($value)) {
        if ($key == 'AND' || $key == 'OR') {
          $return .= self::genWhere([$key => $value], true);
          $first = false;
        }
        continue;
      }
      if (is_object($value)) continue;
      if (is_bool($value)) continue;

      $return .= $assembleEntry($key, $value);
      $first = false;
    }
    $return .= " )";
    return $return;
  }

  static protected function genLimit($limit): string
  {
    if (!$limit) return "";

    if (is_array($limit)) {
      $limit = array_filter($limit, 'is_int', ARRAY_FILTER_USE_KEY);
      if (count($limit) == 1) $limit = $limit[0];

      if (count($limit) > 1) return "\n LIMIT {$limit[0]} {$limit[1]}";
    }

    if (is_int($limit)) return "\n LIMIT {$limit}";
  }
}
