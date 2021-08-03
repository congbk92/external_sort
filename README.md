# external_sort

There are so many great sorting algorithms. Some of them can only be working on the fly. But in practical, memory is limited and can't accommodate all the data for sorting. So the external sorting help to resolve this case by split them to smaller chunk at allowing size, sort and store back to disk. After that, all sorted chunk will be merged. The detail will be vary them.
In this project, those external sort algorithms was implemented to lexicographically sort lines. The main purpose of this project is for study only.

## Description

At this fisrt version, the basic exeternal sorting was implemented as the following:
* In former phase (initial phase), there are 3 threads: Reader, Sorter, Writer. There threads comunicate by an buffer which was synchronize by producer/consumer pattern.
* Merged phase base on k-ways merged algorithm. It help reduce number runs in this phase.
In both those phase, Multithread was used to overlap CPU process and I/O operation as much as possible.

In next version, the following will be consider to implement:
* AlphaSort which one is very fast if there are multiple disks diver such as RAID technique in sever.
* Batch replacement selection which was improved from replacement selection algorithm. It can handle variable length record.

## Getting Started

### Dependencies

* Linux, g++ which supported C++11

### Installing

* run build.sh script

### Executing program

* ./main #input_file_name #output_file_name #memory_limmit

## Help

## Authors
Cong, Nguyen Thanh
* [Skype](https://join.skype.com/invite/heBkJ18SZeo2)

## Version History

* 0.1
    * Initial

## Acknowledgments

* [External Sorting Algorithm: State-of-the-Art and Future Directions] (https://www.researchgate.net/publication/341163308_External_Sorting_Algorithm_State-of-the-Art_and_Future_Directions)
* [External sorting: Run formation revisited] (https://www.researchgate.net/publication/3297177_External_sorting_Run_formation_revisited)
