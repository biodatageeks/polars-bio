## ADDED Requirements

### Requirement: Session option for the processed-rows progress bar

polars-bio SHALL provide the session option `datafusion.bio.progress_bar`,
exported as `pb.POLARS_BIO_PROGRESS_BAR`, that controls the processed-rows
progress bar printed while a LazyFrame returned by polars-bio is collected. The
option SHALL default to `"true"` and SHALL be set with `pb.set_option` and read
with `pb.get_option`. It SHALL apply to every place where polars-bio streams
batches into Polars: file readers, `pb.sql`, range operations and `pb.depth`.

#### Scenario: Default keeps the progress bar
- **WHEN** a user collects a LazyFrame returned by polars-bio without changing the option
- **THEN** `pb.get_option(pb.POLARS_BIO_PROGRESS_BAR)` SHALL return `"true"`
- **AND** the processed-rows progress bar SHALL be written to stderr

#### Scenario: Option turns the progress bar off
- **WHEN** a user calls `pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, False)`
- **AND** collects a LazyFrame returned by a reader, `pb.sql`, a range operation or `pb.depth`
- **THEN** no processed-rows progress bar SHALL be written
- **AND** the collected result SHALL be the same as with the option on

#### Scenario: Option changed after a LazyFrame was created
- **WHEN** a LazyFrame is created while the option is on
- **AND** the option is set to `False` before the LazyFrame is collected
- **THEN** no processed-rows progress bar SHALL be written
- **AND** setting the option back to `True` SHALL show the bar on the next collect

### Requirement: Progress bar option is scoped to polars-bio

The option SHALL affect only progress bars created by polars-bio. It SHALL take
effect without restarting the process and without setting an environment
variable before `tqdm` is imported. The `TQDM_DISABLE` environment variable
SHALL keep suppressing the polars-bio progress bar while the option is on.

#### Scenario: Other tqdm progress bars stay visible
- **WHEN** the option is `False`
- **AND** the application displays its own `tqdm` progress bar
- **THEN** the application's progress bar SHALL still be written

#### Scenario: Option set after import
- **WHEN** a user imports `polars_bio`, collects a LazyFrame, and then sets the option to `False`
- **THEN** later collects in the same process SHALL NOT write the progress bar

#### Scenario: TQDM_DISABLE takes precedence
- **WHEN** the process is started with `TQDM_DISABLE=1`
- **AND** the option has its default value
- **THEN** no processed-rows progress bar SHALL be written

### Requirement: Progress bar option values are validated

`pb.set_option` SHALL accept `True`, `False` and the strings `"true"` and
`"false"` in any letter case for `datafusion.bio.progress_bar`, and SHALL store
the value as lowercase `"true"` or `"false"`. Any other value SHALL raise
`ValueError` and leave the stored value unchanged.

#### Scenario: Boolean and string values
- **WHEN** a user calls `pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, "FALSE")`
- **THEN** `pb.get_option(pb.POLARS_BIO_PROGRESS_BAR)` SHALL return `"false"`

#### Scenario: Mistyped value
- **WHEN** a user calls `pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, "flase")`
- **THEN** `ValueError` SHALL be raised
- **AND** the option SHALL keep its previous value
