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
 * A pool of React\MySQL connections
 */
class Pool
{
  const CS_ROUND_ROBIN = 'round-robin';
  const CS_BY_LOAD = 'load';

  private static $pool;
  private static $poolSize;
  private static $poolPointer = 0;
  private static $poolRequestCounter = [];
  private static $poolConnectionSelector;

  /**
   * Initialize the connections
   *
   * @param  string $uri
   * @param  int    $poolSize
   * @param  [type] $connectionSelector
   * @return void
   */
  static function init(string $uri, int $poolSize = 5, string $connectionSelector = self::CS_BY_LOAD, callable $onError = null)
  {
    self::$poolSize = $poolSize;
    self::$poolConnectionSelector = $connectionSelector;

    self::$pool = [];
    for ($p = 0; $p < self::$poolSize; $p++) {
      self::$pool[$p] = new Connection($uri, $onError);
      self::$poolRequestCounter[$p] = 0;
    }
  }

  static private function shiftPointer()
  {
    self::$poolPointer = (self::$poolPointer + 1) % self::$poolSize;

    if (self::$poolConnectionSelector == self::CS_BY_LOAD) {
      if (self::$poolRequestCounter[self::$poolPointer] == 0) return self::$poolPointer;

      $rcList = self::$poolRequestCounter; // copy
      asort($rcList, SORT_NUMERIC);
      self::$poolPointer = key($rcList);
    }

    return self::$poolPointer;
  }

  static private function pooledCallbackPromise(callable $callback)
  {
    $pointer = self::shiftPointer();
    self::$poolRequestCounter[$pointer]++;
    $connection = self::$pool[$pointer];
    return $callback($connection)->then(function ($result) use ($pointer) {
      self::$poolRequestCounter[$pointer]--;
      return $result;
    });
  }

  /**
   * Performs an async query.
   * 
   * This method returns a promise that will resolve with a `QueryResult` on
   * success or will reject with an `Exception` on error. 
   *
   * @param  string                          $query
   * @return \React\Promise\PromiseInterface
   */
  static function query(string $sql): \React\Promise\PromiseInterface
  {
    return self::pooledCallbackPromise(function (Connection $connection) use ($sql) {
      return $connection->query($sql);
    });
  }

  /**
   * Performs an async query and streams the rows of the result set.
   *
   * This method returns a readable stream that will emit each row of the
   * result set as a `data` event
   * 
   * @param  string                                $query
   * @return \React\Stream\ReadableStreamInterface
   */
  static function queryStream(string $sql): \React\Stream\ReadableStreamInterface
  {
    $pointer = self::shiftPointer();
    self::$poolRequestCounter[$pointer]++;
    $connection = self::$pool[$pointer];
    $stream = $connection->queryStream($sql);
    $stream->on('end', function () use ($pointer) {
      self::$poolRequestCounter[$pointer]--;
    });
    return $stream;
  }

  /**
   * Ping the connection.
   *
   * This method returns a promise that will resolve (with a void value) on
   * success or will reject with an `Exception` on error.
   *
   * @return \React\Promise\PromiseInterface
   */
  static function ping(): \React\Promise\PromiseInterface
  {
    return self::pooledCallbackPromise(function (Connection $connection) {
      return $connection->ping();
    });
  }
}
