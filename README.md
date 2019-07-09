# ravro

A CLI for Avro files, written in Rust.

> **⚠ Under heavily development ⚠**
>
> Please use at your own discretion.

---

## Installation

### Compile from Source

Use `cargo`:

```
cargo build --release
```

### Binaries

There are existing compiled binaries for Windows at the moment.

## Usage

```shell
> rargo get .\bttf.avro

+---------------+--------------+-------------+
| firstName     | lastName     | nickname    |
+---------------+--------------+-------------+
| Marty         | McFly        | Marty       |
+---------------+--------------+-------------+
| Emmett        | Brown        | Doc         |
+---------------+--------------+-------------+
| Biff          | Tannen       | Biff        |
+---------------+--------------+-------------+

> rargo get .\bttf.avro --search McFly

+---------------+--------------+-------------+
| firstName     | lastName     | nickname    |
+---------------+--------------+-------------+
| Marty         | McFly        | Marty       | # McFly should appear in bold here
+---------------+--------------+-------------+

> rargo get .\bttf.avro --fields firstName nickname

+---------------+--------------+
| firstName     | nickname     |
+---------------+--------------+
| Marty         | Marty        |
+---------------+--------------+
| Emmett        | Doc          |
+---------------+--------------+
| Biff          | Biff         |
+---------------+--------------+
```