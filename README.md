# external_sort

There are so many great sorting algorithms. Some of them can only be working on the fly. But in practical, memory is limited and can't accommodate all the data for sorting. So the external sorting help to resolve this case by split them to smaller chunk at allowing size, sort and store back to disk. After that, all sorted chunk will be merged. The detail will be vary them.
In this project, those external sort algorithms was implemented to lexicographically sort lines. The main purpose of this project is for study only.

## Description

At this fisrt version, the basic exeternal sorting was implemented as the following:
* In formation phase (initial phase), there are 3 threads: Reader, Sorter, Writer. There threads comunicate by an buffer which was synchronize by producer/consumer pattern.
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

### Result
* Performance: It sorted 12Gb with 400Mb memory limit. It consumed about 10 minutes. 
* Memory consume: Massif from Valgrind was used to check. It sort 200Mb with 8Mb memory limit. The result was in massif.out.12348 file. It was visualized by massif visualizer as file the following:
![](https://github.com/congbk92/external_sort/blob/884590b239f783783d9dfa012cc7603d6d4d6661/img.jpg?raw=true)

* In almost time, it consumed from 9Mb to 16Mb. It is higher than expected. Because the containers from the standard library tried to allocate more than the current demand. It is an optimization and helped to reduce the times of heap allocation. Btw, there is a pit in this graph. It is time to change the formation phase to the merge phase.

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
