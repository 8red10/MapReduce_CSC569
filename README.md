# Refactored Structure for CSC 569 Lab 4

## Project Organization

describe something about the organization and structure of the directories of the project.
- i.e. what does `cmd` contain
- i.e. what does `internal` contain and why is it an important keyword

## Future Improvements
- there should be a node info struct that only contains information necessary to facilitate gossip
    - i.e. node info only has stuff for memberlist operations
    - node info can be part of the overall node struct
- there should be an overall node struct that contains information necessary to facilitate all node operations
    - this struct would be declared at the application level 
    - part of this struct would be the node info
        - for memberlist and gossip facilitation
    - part of this struct would be all timers 
        - for message passing 
    - essentially this struct would be the only variable in main and would replace the need for any global variable
        - any global variable could just be a part of this struct
            - node info
            - memberlist
            - all timers
            - log
            - waiting logentry
        - then would just pass a pointer to this struct wherever globals are being used
    - performance concerns
        - this would decrease the concurrency due to a single mutex protecting this whole struct
        - OR have a mutex per important item in the struct so that can access the struct contents individually without affecting other accesses
