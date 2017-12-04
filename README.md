# CS425 MP4
group 28:
- Wei Ren (weiren2)
- Qishan Zhu (qishanz2)

## Compiling instruction
**Go version needs to be higher than 1.9.1**.

Use `go get` to get dependencies (protocol buffer).

Then `cd` to each sub-directory and run `go build` will compile the program named correspondingly.

The programs are seperated into three parts:

 - Master : managing jobs
 - Worker : actually running jobs
 - Client : submitting jobs

Deploy all the code/executable across all nodes. There will be prompts in the terminal.


## Assumption
- Hostname on vm will always follow the format of `fa17-cs425-g28-%02d.cs.illinois.edu` so that our program can get node id from hostname.
