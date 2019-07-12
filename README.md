# ravro

![Version 0.1.0](https://img.shields.io/badge/version-0.1.0-green.svg)

A CLI for [Apache Avro](https://avro.apache.org/) manipulations.

![Screenshot](./assets/image.png)

> **⚠ Under heavy development ⚠**
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
They can be downloaded from the [releases](https://github.com/guywald1/ravro/releases) page.

## Usage

```shell
> # Retrieve all columns for a list of records
> ravro get .\test_assets\bttf.avro

+---------------+--------------+-------------+
| firstName     | lastName     | nickname    |
+---------------+--------------+-------------+
| Marty         | McFly        | Marty       |
+---------------+--------------+-------------+
| Emmett        | Brown        | Doc         |
+---------------+--------------+-------------+
| Biff          | Tannen       | Biff        |
+---------------+--------------+-------------+

> # Search (using regular expressions)
> ravro get .\test_assets\bttf.avro --search McFly

+---------------+--------------+-------------+
| firstName     | lastName     | nickname    |
+---------------+--------------+-------------+
| Marty         | McFly        | Marty       | # McFly should appear in bold green here
+---------------+--------------+-------------+

> # Select only some columns
> ravro get .\test_assets\bttf.avro --fields firstName nickname

+---------------+--------------+
| firstName     | nickname     |
+---------------+--------------+
| Marty         | Marty        |
+---------------+--------------+
| Emmett        | Doc          |
+---------------+--------------+
| Biff          | Biff         |
+---------------+--------------+

> # Select the first 2 columns
> ravro get .\test_assets\bttf*.avro --fields firstName nickname --take 2

+---------------+--------------+
| firstName     | nickname     |
+---------------+--------------+
| Marty         | Marty        |
+---------------+--------------+
| Emmett        | Doc          |
+---------------+--------------+
```

## Options

- `fields (f)` - The list (separated by spaces) of the fields you wish to retrieve
- `path (p)` - The glob to one or multiple Avro files
- `search (s)` - The regular expression to filter and display only rows with columns that contain matching values. The matching fields will be highlighed
- `take (t)` - The number of records you wish to retrieve
- `codec (c)` - The codec for decompression - omit for no codec, or specify "deflate"

## TODO

- Extract CLI functionality into a library
- Configurable display formats (CSV, JSON, etc.)
- Avro generation from JSON
- Schema
- `Snappy` codec

## Caveats

- The schema is inferred based on the first record it finds. This may not be desired for some use-cases
- Only supports top-level records at the moment

---

## Contributions

Are very welcome! I am by no means an expert on Spark, Avro or even Rust and there is _much_ to be improved here.


## Thanks 🙏

- [avro-rs](https://github.com/flavray/avro-rs)
