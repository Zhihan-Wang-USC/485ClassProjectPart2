# Class Project: Part 2

This repo is the base codes of the ClassProject of 2023Spring CSCI485.

## Getting Started
You need to copy and paste the code of Part 1 to this project. 
Your codes need to be put under `src/CSCI485ClassProject/` and its subdirectories. 
Once finished, you can verify it by running the unit test of Part1.

## Project Structure Overview

- `lib`: the dependencies of the project
- `src`: the source codes of the project
  - `CSCI485ClassProject`: root package of the project
    - `models`: package storing some defined representations of models in the database.
    - `test`: package for unit tests
    
## Codes to implement
Under `src/CSCI485ClassProject`, there are 2 classes to finish:
- `Cursor`: `Cursor` implementation
- `RecordsImpl`: implementation of an interface class `Records`

## Run Tests on macOS/Linux using `make`

If you are developing in `macOS/Linux` environment(recommended), we provide `Makefile` for you to run tests quickly.

To run tests of part1, use command
```shell
make part1Test
```

To run tests of part2, use command
```shell
make part2Test
```

As you may have different project structures, Makefile may not work in your implementation. In this case, you can change the `sources` variable in Makefile by adding the name of the java files you created to it.
Note that the order of the file should align with the class dependency relationship, i.e. class `A` imports `B`, then `B.java` should be in front of `A.java` in `sources`.