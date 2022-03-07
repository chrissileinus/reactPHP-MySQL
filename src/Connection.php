<?php
/*
 * Created on Fri Mar 04 2022
 *
 * Copyright (c) 2022 Christian Backus (Chrissileinus)
 *
 * For the full copyright and license information, please view the LICENSE file that was distributed with this source code.
 */

namespace Chrissileinus\React\MySQL;

use React\MySQL;

/**
 * A single React\MySQL connection
 */
class Connection implements MySQL\ConnectionInterface, \Evenement\EventEmitterInterface
{
  use \Evenement\EventEmitterTrait;

  protected $connection;
  protected $onError;

  function __construct(string $sqlUri, callable $onError = null)
  {
    $this->onError = $onError;

    $sqlFactory = new MySQL\Factory();

    $this->connection = $sqlFactory->createLazyConnection($sqlUri);
    $this->connection->on('error', function ($e) {
      $this->emit('error', [$e]);
    });
    $this->connection->on('close', function () {
      $this->emit('close');
    });
  }

  public function query($sql, $params = [])
  {
    return $this->connection->query($sql, $params)->then(
      function (MySQL\QueryResult $result) {
        return $result;
      },
      function (\Throwable $th) use ($sql) {
        if (is_callable($this->onError)) {
          return call_user_func($this->onError, $th, $sql);
        }
        throw $th;
      }
    );
  }

  public function queryStream($sql, $params = [])
  {
    $stream = $this->connection->queryStream($sql, $params);
    $stream->on('error', function (\Throwable $th) use ($sql) {
      return call_user_func($this->onError, $th, $sql);
    });
    return $stream;
  }

  public function ping()
  {
    return $this->connection->ping();
  }

  public function quit()
  {
    return $this->connection->quit();
  }

  public function close()
  {
    return $this->connection->close();
  }
}
