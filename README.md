# Ask Manta

Ask Manta is a job runner for Joyent's Manta cloud storage and map-reduce service. Like Joyent's own `mjob`, but higher-level. Uploads dependencies, tracks running jobs, returns any errors and so on.

## CLI

Ask Manta is available on the commandline: the `askmanta` command (just ask manta). Run `askmanta -h` to get an idea of what subcommands are available.

## Job files

Ask Manta job definitions are YAML files will look reasonably familiar if you've used Manta step definitions before.