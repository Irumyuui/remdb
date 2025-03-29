# RemDB

## KV 分离

实现 kv 分离。对于大的 value ，则独立缓存到指定的 vlog 目录中的 vlog 文件，每一个 vlog entry 在文件中以追加的形式写入，以以下格式缓存。

```text
    +----------------+
    | seq: u64       |
    +----------------+
    | key len: u32   |
    +----------------+
    | value len: u32 |
    +----------------+
    | key            |
    +----------------+
    | value          |
    +----------------+
    | check sum: u32 |
    +----------------+
```

当一个 value 被写入到 vlog 文件中，将返回一个 value pointer ，该 ptr 代替 value 作为 lsm 中的 value ，等待读取时索引到 vlog 文件中，由 `io_uring` 提供并发读取支持。

### value log gc

对于 value log gc ，每当采取一定的计分规则触发对某一个 value log 的 rewrite ，此时依然是将该 vlog 文件直接追加写入到新的 vlog 文件中，并在未来某一时期计划好需要删除被重写的 vlog 文件。每个被重写的 entry 将首先检查是否依然还位于 lsm 中，如果存在，则首先重写，等待所有 entry 重写完毕之后，**就地**更新 table 文件中的所指定的 value pointer 为被重写的 entry 其 value pointer。

这个过程涉及到一定的崩溃错误处理。

- 若还没 gc 就崩溃，那么此时则对于该实例并无影响；
- 若正在重写 entires 阶段，那么则已经写入的 entries 作为无效值；
- 若正在重写 value ptr， 因为文件延后删除，所以新旧文件都存在，无影响；
- 若文件未删除，则此时旧文件将在新一次 gc 触发时进行回收；

因此并不用考虑错误发生。

## TODO

Value Log 目前实验的回收方案有两种，一种是从旧文件触发 GC 并扫描到新文件，一种是运行时统计每个文件的触发时机。

目前正在实验的是，同时使用两种计分方式

- 首先记录 compact 时被丢弃的 kv ，其指向的 vlog 文件，触发 vlog gc 时得分最高与次高文件将被回收，如果这个得分超过一个阈值的话
- 其次每次访问 vlog 时需要临时记录一下每个文件被访问的次数，最少被访问的文件也将考虑被回收，因为他们可能已经是无法触及的记录了

第一个记录解决的是 kv 被丢弃的问题，第二个则是考虑到可能存在不安全关闭的问题，当重启时这些文件将可能不会被记录在其中。由于第二个可能会导致引入一些不必要的开销，因此目前正在实验是否采用从最旧文件直接 GC 的方式代替第二种 GC 。

## Reference

![WiscKey: Separating Keys from Values  
in SSD-conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)