<?php
/*
 * Created on Wed Feb 23 2022
 *
 * Copyright (c) 2022 Christian Backus (Chrissileinus)
 *
 * For the full copyright and license information, please view the LICENSE file that was distributed with this source code.
 */

namespace Chrissileinus\React\MySQL;

use React\MySQL;

/**
 * A pool of React\MySQL connections
 */
class Pool
{
  private static $pool;
  private static $poolSize;
  private static $poolPointer = 0;

  /**
   * Initialize the connections
   *
   * @param  string $sqlUri
   * @param  int    $poolSize
   * @return void
   */
  static function init(string $sqlUri, int $poolSize = 5)
  {
    self::$poolSize = $poolSize;

    self::$pool = [];
    for ($p = 0; $p < self::$poolSize; $p++) {
      self::$pool[$p] = new Connection($sqlUri);
    }
  }

  /**
   * Get a connection
   *
   * @return \React\MySQL\ConnectionInterface
   */
  static function get(): MySQL\ConnectionInterface
  {
    return self::$pool[self::nextPointerByLoad()];
  }

  static private function nextPointer()
  {
    return (self::$poolPointer + 1) % self::$poolSize;
  }
  static private function nextPointerByLoad()
  {
    $pointer = self::nextPointer();

    if (self::$pool[$pointer]->busyCounter() == 0) return $pointer;

    $min = null;
    $busyCounters = [];
    for ($p = 0; $p < self::$poolSize; $p++) {
      $busyCounters[$p] = self::$pool[$p]->busyCounter();

      if (self::$pool[$p]->busyCounter() == 0) return $p;

      if ($busyCounters[$p] < $min || $min === null) {
        $min = $busyCounters[$p];
        $pointer = $p;
      }
    }

    return $pointer;
  }
}
