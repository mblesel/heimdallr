# heimdallr - A Rust message passing interface

## Building heimdallr
After cloning the repository a simple `cargo build --release` command will build the heimdallr library, the heimdallrd daemon and the included test applications.

## Running heimdallr
All built binaries can be found in the `target/release` directory.
For a simple test it is recommended to run the included `partdiff` application.

To run a heimdallr application it is required to have one instance of the daemon `heimdallrd` running on a computing node.

`./heimdallrd --partition home --name home1`
will start the daemon process under the partition name `home` and the daemon name `home1`.

Every heimdallr client application needs to specify the targeted partition, the name of the targeted daemon process and the process count of the application run.

`./partdiff --partition home --node home1 --jobs 4 --args 1 2 100 2 2 100`
will start one process of a 4 process job for the partdiff application on partition `home` and for the daemon process `home1`.
