# topic-copy
Copy topics to another branch, possibly altering properties (e.g. view to physical topic)

Its original use case was to copy topics created by a remote topic view to a physical topic, allowing a copy of the topic to remain if the remote connection is lost.

## Building

`mvn clean package`

## Running

`java -jar TopicCopy-1.0-main.jar [options]`

## Options

### `--help`
Show overview of options, including default values.

### `--url`
#### Default: `ws://localhost:8080`
Diffusion host url

### `--principal`
#### Default: `control`
Principal (username) to use when connecting to Diffusion. Typically "`control`" or "`admin`".

### `--credentials`
#### Default: `password`%^[
Password (or other crednetials) to use when connecting to Diffusion.

### `--source` (Required)
Topic selector specifying the source topics to be copied.

### `--target` (Required)
Topic root under which the selected topics will be copied.

### `--trim`
#### Default: `0`
Number of leading path elements to remove from the source topic path when generating the target topic path.

e.g.

with `--source '?view//' --target 'local' --trim 0`, then `view/foo/bar` maps to `local/view/foo/bar`

with `--source '?view//' --target 'local' --trim 1`, then `view/foo/bar` maps to `local/foo/bar`


### `--remove`
#### Default: `_VIEW,REMOVAL`
A list of topic properties that will be removed from the topic specification when copying from the source to destination topics. The source topic is not modified.

This parameter may be given multiple times, or as a comma-separated list of properties.
