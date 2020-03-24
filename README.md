<!--
   Copyright 2020 Viseca Card Services SA

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

# Flink templates and common streaming analytics resources developed by Viseca Card Services SA

## Welcome to __ch.viseca.flink:flink-commons__

In early 2020 we, Viseca Card Services SA, decided to share part of our efforts in the area of stream processing and Flink 
with the general public. 

The libraries and modules presented here help us jump start Flink stream processing projects, especially with:
* bridging the initial learning curve necessary 
* having a common setup for building projects that can easily be adjusted in a single place (see 'starters' further down)
* taking care of time-to-market aspects
* integration with CI/CD procedures
* and others

Feedback from 'the public' is welcome.

### What it is not yet
* Well documented (although most parts have code documentation)
* Stable API: Interfaces are subject to frequent change, we don't make any guarantees for backwards compatibility
* There is no 'Getting Started' section yet
* Publication of pre-built artifacts

## Subfolders description
* __archetypes__   - Maven archtypes to create Flink Scala Jobs (all archetypes integrate the below mentioned __flink-scala-commons-starter__)
    * __scala-only-archetype__ - prepares a simple Scala project
    * __flink-library-archetype__ - prepares a Flink library used to extend functionality (not for jobs)
    * __flink-job-archetype__ - prepares a Flink job library (without Spring Boot)
    * __flink-spring-job-archetype__ - prepares a Flink job library including integration with Spring Boot 
    dependency injection and configuration
    * __flink-project-container-archetype__ - prepares a multi-module container project
* __libraries__ - libraries to implement common stream processing concerns
    * __flink-scala-commons__ - common extensions to the Flink ecosystem
        * __ch.viseca.flink.conversion__ - conversion helpers
        * __ch.viseca.flink.featureChain__ - implementation of 
            * discriminated union types and
            * generalization of components for feature chains : [... the pattern was introduced at the FlinkForward SF 2017 conference](https://www.youtube.com/watch?v=Do7C4UJyWCM)  
        * __ch.viseca.flink.jobSetup__ - runtime pluggable/replaceable sources, sinks, components for Flink jobs
        * __ch.viseca.flink.metaModel__ - extends jobSetup with a meta model
        * __ch.viseca.flink.logging__ - logging integration
        * __ch.viseca.flink.operators__ - common operator functions
        * __ch.viseca.flink.waterMarkAssignment__ - watermark assigners for streamed batch sources 
* __samples__ - sample modules to demonstrate principles and for documentation
    * __flink-samples-featureChain-noDep__ - usage example for a feature chain and discriminated unions  
    * __flink-samples-spring-featureChain__ - the same example with Spring Boot integration
* __starters__ - optional parent POMs for a common build/dependency setup
    * __flink-scala-commons-starter__ - a parent POM module 
        * that configures build processes and dependencies by means of a set of Maven profiles
        * thus Maven profiles are enabled using tag-files (look for the '_inheritedProfiles_' folders)
        * by deriving a project from the starter POM you subscribe to a tested set of build instructions and 
        common dependency versions,
        * for changing to newer versions of e.g. Flink it suffices to derive from a different versions of the start
        * the same is true for corrections in the build processes - there is no need to adjust all involved projects    
