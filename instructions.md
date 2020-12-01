# RepCRec

## Running RepCRec from `RepCRec.rpz`

My implementation of RepCRec has been reproducibly packaged using
`reprozip`. Given `RepCRec.rpz`, one can follow the instructions
below to (1) reproducibly run the set of tests I've provided illustrating
my system's execution, and (2) execute instructions for a single
`RepCRec` run from a file.

First, I give instructions for running on a linux machine. Then
I give instructions for running on a Mac. Note that while these instructions give
one way to unpack and run  `RepCRec.rpz`, there are other unpackers,
and other ways to interact with the package. That said, this approach
is simple and uses `reprounzip` effectively.

### Running on Linux

1. **Setup**: Unpack the package with `reprounzip directory setup RepCRec.rpz RepCRec`
2. **Run the provided tests**: Run the provided tests using `reprounzip directory run RepCRec 0`
3. **Execute custom instructions**: To execute custom instructions in a file `new_instructions.txt`,
there are two steps:
    1. **Upload the new instructions**: Upload the new instruction file `new_instructions.txt` using
       the command `reprounzip directory upload RepCRec new_instructions.txt:arg1`
    2. **Run RepCRec using these instructions**: Execute using the command `reprounzip directory run RepCRec 1`
4. **Teardown**: To tear down, simply run `reprounzip directory destroy RepCRec`.

### Running on Mac using Docker

These instructions assume you have already installed `docker`. 

1. **Setup**: Unpack the package with `reprounzip docker setup RepCRec.rpz RepCRec`
2. **Run the provided tests**: Run the provided tests using `reprounzip docker run RepCRec 0`
3. **Execute custom instructions**: To execute custom instructions in a file `new_instructions.txt`,
there are two steps:
    1. **Upload the new instructions**: Upload the new instruction file `new_instructions.txt` using
       the command `reprounzip docker upload RepCRec new_instructions.txt:arg1`
    2. **Run RepCRec using these instructions**: Execute using the command `reprounzip docker run RepCRec 1`
4. **Teardown**: To tear down, simply run `reprounzip docker destroy RepCRec`.