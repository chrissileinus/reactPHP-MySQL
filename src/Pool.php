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

    $sqlFactory = new MySQL\Factory();

    self::$pool = [];
    for ($p = 0; $p < $poolSize; $p++) {
      self::$pool[$p] = $sqlFactory->createLazyConnection($sqlUri);
    }
  }

  /**
   * Get a connection
   *
   * @return \React\MySQL\ConnectionInterface
   */
  static function get(): MySQL\ConnectionInterface
  {
    self::$poolPointer = (self::$poolPointer + 1) % self::$poolSize;

    return self::$pool[self::$poolPointer];
  }
}
