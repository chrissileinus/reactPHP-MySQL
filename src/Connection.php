<?php
/*
 * Created on Fri Mar 04 2022
 *
 * Copyright (c) 2022 Christian Backus (Chrissileinus)
 *
 * For the full copyright and license information, please view the LICENSE file that was distributed with this source code.
 */

namespace Chrissileinus\React\MySQL;

use Evenement\EventEmitterTrait;
use React\MySQL;
use React\MySQL\QueryResult;
use React\Stream\ReadableStreamInterface;

/**
 * A pool of React\MySQL connections
 */
class Connection implements MySQL\ConnectionInterface
{
  use EventEmitterTrait;

  protected $connection;
  protected int $busyCounter = 0;

  function __construct(string $sqlUri)
  {


    $sqlFactory = new MySQL\Factory();

    $this->connection = $sqlFactory->createLazyConnection($sqlUri);
    $this->connection->on('error', function ($e) {
      $this->emit('error', [$e]);
    });
    $this->connection->on('close', function () {
      $this->emit('close');
    });
  }

  public function busyCounter()
  {
    return $this->busyCounter;
  }

  public function query($sql, $params = [])
  {
    $this->busyCounter++;
    return $this->connection->query($sql, $params)->then(
      function (QueryResult $result) {
        $this->busyCounter--;
        return $result;
      }
    );
  }

  public function queryStream($sql, $params = [])
  {
    $this->busyCounter++;
    $stream = $this->connection->queryStream($sql, $params);
    $stream->on('end', function () {
      $this->busyCounter--;
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
