package main

/*

redis data structure

dev_list - HASH IP [pause|run]
ip_lock.Q.IP - redmutex key
ip_last_result.Q.IP - ok:timestart:timedone | error:timestart:timeerror:error msg
ip_data.Q.IP - queue Q HASH  someKey value otherKey.index value ...
ip_keys.Q.IP - queue Q HASH  someKey timestart_ms:timestop_ms otherKey timestart_ms:timestop_ms ...

ip_oids.Q.IP - queue Q HASH  someKey oid:itemType:valueType:opt:optv ...


*/

import (
  "regexp"
  _ "sync"
  "fmt"
  "os"
  "time"
  "errors"
  _ "strings"
  "strconv"
  "github.com/gomodule/redigo/redis"
  _ "github.com/ShyLionTjmn/redmutex"
  "github.com/fatih/color"
  "sort"
)

const STALE_RUN_DUR = 20*time.Second

var testErr = errors.New("Test error")

const REDIS_SOCKET="/tmp/redis.sock"
const REDIS_DB="0"

var red_db string=REDIS_DB

const DEFAULT_ACTION="info"

const USAGE=`Usage: mapper_info command ...
	info:		overall status of devs
	topkeys IP:	top 10 slowest keys
`

type Key_info struct {
  Key		string
  Queue		int64
  Ms		int64
}

type SortByMs []Key_info

func (a SortByMs) Len() int		{ return len(a) }
func (a SortByMs) Swap(i, j int)	{ a[i], a[j] = a[j], a[i] }
func (a SortByMs) Less(i, j int) bool	{ return a[j].Ms < a[i].Ms }

func main() {
  var red redis.Conn
  var err error

  var gomap_start_unix int64=0
  var gomap_run_unix int64=0

  defer func() {
    if err != nil {
      color.New(color.FgRed).Fprintln(os.Stderr, err.Error())
      os.Exit(1)
    }
  } ()

  ip_regex := regexp.MustCompile(`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$`)
  qstr_regex := regexp.MustCompile(`^(\d+):(\d+):([^:]+):(.*)$`)
  run_regex := regexp.MustCompile(`^(\d+):(\d+)$`)

  keys_regex := regexp.MustCompile(`^(\d+):(\d+):(one|table):(int|uns|oid|str|hex)$`)

  red, err = redis.Dial("unix", "/tmp/redis.sock")
  if err != nil { return }

  defer func() {
    red.Close()
  } ()

  _, err = red.Do("SELECT", red_db)
  if err != nil { return }

  gomap_start_unix, err = redis.Int64(red.Do("GET", "gomapper.start"))
  if err == redis.ErrNil {
    err = errors.New("No gomapper.start key in redis, cannot continue")
  }
  if err != nil { return }

  var gomap_run_str string
  gomap_run_str, err = redis.String(red.Do("GET", "gomapper.run"))
  if err != nil && err != redis.ErrNil { return }

  if err == nil {
    m := run_regex.FindStringSubmatch(gomap_run_str)
    if m == nil {
      err = errors.New("Bad gomapper.run contents")
      return
    }
    var run_check int64
    run_check, err = strconv.ParseInt(m[1], 10, 64)
    if err != nil { return }
    gomap_run_unix, err = strconv.ParseInt(m[2], 10, 64)
    if err != nil { return }

    if run_check != gomap_start_unix {
      err = errors.New("gomapper.run does not match gomapper.start")
      return
    }

    if time.Unix(gomap_run_unix, 0).Add(60*time.Second).Before(time.Now()) {
      err = errors.New("gomapper.run is way to old")
      return
    }
  } else {
    //no gomapper.run key
    color.Yellow("No gomapper.run key, data probably outdated")
  }

  var queue_list []int64

  queue_list, err = redis.Int64s(red.Do("LRANGE", "gomapper.queues", "0", "-1"))
  if err != nil && err != redis.ErrNil { return }
  if err == redis.ErrNil {
    err = errors.New("No gomapper.queues key in redis, cannot continue")
    return
  }

  if len(queue_list) <= 1 {
    err = errors.New("No queues data in gomapper.queues key in redis, cannot continue")
    return
  }

  if queue_list[0] != gomap_start_unix {
    err = errors.New("Queues data in gomapper.queues key does not match gomapper.start")
    return
  }

  queue_list = queue_list[1:]

  zero_queue_found := false

  for _, q := range queue_list {
    if q == 0 {
      zero_queue_found=true
      break
    }
  }

  if !zero_queue_found {
    err = errors.New("No zero queue in gomapper.queues key in redis, cannot continue")
    return
  }

  var dev_list map[string]string

  dev_list, err = redis.StringMap(red.Do("HGETALL", "dev_list"))
  if err != nil { return }

  var action string

  args := os.Args

  if len(args) < 2 {
    action=DEFAULT_ACTION
  } else {
    action, args = args[1], args[2:]
  }

  if action == "info" {
    var invalid_devices int
    var run_devices int
    var pause_devices int
    var other_devices int

    var run_dev_good int
    var run_dev_warn int
    var run_dev_error int
    var run_dev_invalid int
    var run_dev_quit int
    var run_dev_stale int

    for dev_ip, dev_run := range dev_list {
      ip_valid := ip_regex.MatchString(dev_ip)
      if ip_valid {
        if dev_run == "run" {
          run_devices++
          var ip_queues map[string]string

          ip_queues, err = redis.StringMap(red.Do("HGETALL", "ip_queues."+dev_ip))
          if err != nil { return }

          var zero_queue_ok bool
          var good_queues int
          var error_queues int
          var stale_queues int
          var quit_queues int
          var invalid_queues int

          for _, q := range queue_list {
            qstr, qexists := ip_queues[ strconv.FormatInt(q, 10) ]
            if !qexists {
              invalid_queues++
              continue
            }
            m := qstr_regex.FindStringSubmatch(qstr)
            if m != nil {
              queue_report_unix, _err := strconv.ParseInt(m[1], 10, 64)
              if _err != nil {
                invalid_queues++
                continue
              }
              queue_nextrun_unix, _err := strconv.ParseInt(m[2], 10, 64)
              if _err != nil {
                invalid_queues++
                continue
              }

              queue_report_time := time.Unix(queue_report_unix, 0)
              queue_nextrun_time := time.Unix(queue_nextrun_unix, 0)

              switch m[3] {
              case "run":
                if queue_report_time.Add(STALE_RUN_DUR).Before(time.Now()) {
                  //queue is stale, should report at most SNMP_TIMEOUT+1 seconds ago
                  stale_queues++
                } else {
                  good_queues++
                  if q == 0 { zero_queue_ok=true }
                }

              case "good_sleep":
                if queue_nextrun_time.Add(time.Second).Before(time.Now()) {
                  //queue is stale, should have been restarted cycle already
                  stale_queues++
                } else {
                  good_queues++
                  if q == 0 { zero_queue_ok=true }
                }

              case "error_sleep":
                if queue_nextrun_time.Add(time.Second).Before(time.Now()) {
                  //queue is stale, should have been restarted cycle already
                  stale_queues++
                } else {
                  error_queues++
                }

              case "quit":
                //queue terminated by some reason (programm exit or sysOID change)
                quit_queues++
              }
            } else {
              invalid_queues++
            }
          }

          if good_queues == len(queue_list) && zero_queue_ok {
            run_dev_good++
          } else {
            if invalid_queues > 0 {
              run_dev_invalid ++
            } else if stale_queues > 0 {
              run_dev_stale++
            } else if quit_queues > 0 {
              if quit_queues == len(queue_list) {
                run_dev_quit++
              } else {
                run_dev_stale++
              }
            } else if zero_queue_ok {
              run_dev_warn++
            } else {
              run_dev_error++
            }
          }

        } else if dev_run == "pause" {
          pause_devices++
        } else {
          other_devices++
          fmt.Println("Other:", dev_ip, dev_run)
        }
      } else {
        invalid_devices++
        fmt.Println("Invalid:", dev_ip, dev_run)
      }
    }
    fmt.Printf("Total devs in dev_list: %d\n", len(dev_list))
    fmt.Printf("Should run: %d\n", run_devices)
    fmt.Printf("Paused: %d\n", pause_devices)
    fmt.Printf("Other: %d\n", other_devices)
    fmt.Printf("Invalid: %d\n", invalid_devices)
    fmt.Printf("Runtime info:\n")
    fmt.Printf("Ok: %d\n", run_dev_good)
    fmt.Printf("Warn: %d\n", run_dev_warn)
    fmt.Printf("Error: %d\n", run_dev_error)
    fmt.Printf("Stale: %d\n", run_dev_stale)
    fmt.Printf("Quit: %d\n", run_dev_quit)
    fmt.Printf("Invalid: %d\n", run_dev_invalid)
  } else if action == "topkeys" {
    if len(args) == 0 {
      err = errors.New("no ip specified")
      return
    }
    for _, dev_ip := range args {
      if !ip_regex.MatchString(dev_ip) {
        err = errors.New("bad ip specified")
        return
      }
    }
    for _, dev_ip := range args {
      var dev_keys_check = make(map[string]bool)
      var dev_keys = make(SortByMs, 0)

      for _, queue := range queue_list {
        ip_keys_key := fmt.Sprintf("ip_keys.%d.%s", queue, dev_ip)
        var ip_keys map[string]string
        ip_keys, err = redis.StringMap(red.Do("HGETALL", ip_keys_key))
        if err != nil && err != redis.ErrNil { return }
        if err == redis.ErrNil || ip_keys == nil {
          err = errors.New(fmt.Sprintf("Missing queue %d data for %s", queue, dev_ip))
          return
        }
        for key, key_str := range ip_keys {
          if key == "_count" { continue }
          m := keys_regex.FindStringSubmatch(key_str)
          if m == nil {
            err = errors.New("Key ip_keys_key is invalid")
            return
          }
          var key_start_ms, key_stop_ms int64
          key_start_ms, err = strconv.ParseInt(m[1], 10, 64)
          if err != nil { return }
          key_stop_ms, err = strconv.ParseInt(m[2], 10, 64)
          if err != nil { return }
          _, exists := dev_keys_check[key]
          if exists {
            err = errors.New("Duplicate key "+key)
            return
          }
          dev_keys_check[key] = true
          ki := Key_info{Key: key, Queue: queue, Ms: key_stop_ms-key_start_ms}
          dev_keys = append(dev_keys, ki)
        }
      }

      sort.Sort(dev_keys)

      fmt.Println("Host:", dev_ip)
      for i, ki := range dev_keys {
        fmt.Println("  Queue:", ki.Queue, "\tms:", ki.Ms, "\tKey:", ki.Key)

        if i > 9 { break }
      }

    }
  } else {
    fmt.Fprintln(os.Stderr, "Unknown action: "+action)
    fmt.Fprintln(os.Stderr, USAGE)
  }

}
