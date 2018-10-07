# MicroTune
µTune: Auto-Tuned Threading for OLDI Microservices

µTune is a system that has two features:
(1) A framework that builds upon Google's gRPC to enable microservices to abstract threading model design from service code.
(2) An automatic load adaptation system that determines load via event-based load monitoring and tunes both the threading model and thread pool sizes in response to load changes. 
µTune was originally written to improve the tail latency of modern microservices. You can find more details about µTune in our OSDI paper (http://akshithasriraman.eecs.umich.edu/pubs/OSDI2018-%CE%BCTune-preprint.pdf).

# License & Copyright
µTune is free software; you can redistribute it and/or modify it under the terms of the BSD License as published by the Open Source Initiative, revised version.

µTune was originally written by Akshitha Sriraman at the University of Michigan, and per the the University of Michigan policy, the copyright of this original code remains with the Trustees of the University of Michigan.

If you use this software in your work, we request that you cite the µTune paper ("μTune: Auto-Tuned Threading for OLDI Microservices", Akshitha Sriraman and Thomas F. Wenisch, 13th USENIX Symposium on Operating Systems Design and Implementation, October 2018), and that you send us a citation of your work.

# Installation
To install µTune, run the following command from the home directory:
$ python install.py

# Issues
If you have issues with installation or running μTune, please raise an issue in this github repository or email akshitha@umich.edu.

# Maintenance
Frequent code or data pushes to this repository are likely. Please pull from this repository for the latest update.
MicroTune is developed and maintained by Akshitha Sriraman.
