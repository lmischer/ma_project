# Computational Analysis of Šams ad-Dīn Muḥammad Saḫāwī *Aḍ-Ḍawʾ al-lāmiʿ li-ahl al-qarn at-tās*

## The Project

This project explores the possibilities of digital humanities to develop a computational analysis approach to biographical dictionaries from the Arabic written tradition.
These sources are particularly interesting for quantitative analysis, but approaches without the support of computers have, so far, been very time-consuming.
Until now, computer-based approaches have lacked tacked tools for Arabic natural language processing (NLP) - such as named entity recognition (NER) - that have long been available for English.
This project, therefore, investigates the suitability of tools for Arabic to apply computational analysis of the Digital Humanities to problems from Arabic studies, creating opportunities to answer old and new research questions dealing with biographical dictionaries.
This approach was developed using the *Aḍ-Ḍawʾ al-lāmiʿ li-ahl al-qarn at-tāsiʿ* from Šams ad-Dīn Muḥammad Saḫāwī (d. AH 902/AD 1497) to test it.
The approach was applied to two editions of that text.
One edition is retrieved from Shamela ([https://shamela.ws/](https://shamela.ws/)) and the other from the openITI ([https://github.com/OpenITI](https://github.com/OpenITI)) corpus.

### Methods

First, the text is preprocessed by parsing the documents to extract individual entries and normalising the Arabic script.
After preprocessing, a fully automated annotation and extraction of information are executed.
Further, suitable sources of additional data for enriching the two texts and supporting the analyses are collected and integrated.
Finally, it is possible to analyse and visualise different aspects of the texts: dates, locations and persons.
Methods for visualising the results and evaluating their quality are presented.
Overall quality is measured with 62 randomly selected entries from the openITI version that have been manually annotated by verifying their automatic annotation stemming from the developed program.
The steps are presented in the same order in which they are applied within the approach.
The program is implemented in Python using the Pandas framework to facilitate working with structured data.
Python has libraries for working with textual data, Arabic language and visualisations, furthermore data collections, making it a suitable tool to meet the demands of this project.
The individual libraries further used are introduced in the context of the methods in which they are applied.
In addition, Dask is used to parallelise the program and speed up processing.
For Natural Language Processing (NLP), CAMeL Tools is used ([https://github.com/CAMeL-Lab/camel_tools](https://github.com/CAMeL-Lab/camel_tools)).
CAMeL Tools is selected because it is versatile, covering multiple NLP tasks needed in this project.
The language model *calima-msa-r13* is applied for the tokenisation and disambiguation tasks, which obtains lemmas and part-of-speech tags.
For the NER, CAMeL Tools provides several language models for direct use.
This project uses the classical Arabic model optimised for the NER, which was trained on the openITI corpus.
The Arabic language varies depending on the dialect used - e.g. Modern Standard Arabic or Classic Arabic - and the genre.
Since the aḍ-Ḍawʾ is from the end of the classic period, the approach applies the Classic Arabic language model wherever possible.

### Example Results

#### Dates

Pie Chart Date Categories of OpenITI Edition:
![Pie Chart Date Categories of OpenITI Edition](https://user-images.githubusercontent.com/17195420/213867051-b8521cf6-126b-4446-85dc-6ae82a0dc48a.png)

Histogram of Distribution of Dates in OpenITI Edition:
![Histogram of Distribution of Dates in OpenITI Edition](https://user-images.githubusercontent.com/17195420/213867212-5e931f49-bd7d-48c1-90a2-5a4b39dd828a.png)

#### Locations

Mapped Locations from OpenITI Edition Enriched by aṯ-Ṯurayyā Gazetteer:
![Mapped Locations from OpenITI Edition Enriched by aṯ-Ṯurayyā Gazetteer](https://user-images.githubusercontent.com/17195420/213867073-d3867706-8b74-422a-8c1e-3e3edf062d7e.png)

Mapped Locations from OpenITI Edition Enriched by Wikidata Gazetteer:
![Mapped Locations from OpenITI Edition Enriched by Wikidata Gazetteer](https://user-images.githubusercontent.com/17195420/213867085-79b1de53-5223-48ed-8fc6-d463225d21c7.png)

To see interactive map:
```shell
python -m http.server --dir results/openiti/
```
Select `gazetteer_map.html`


## Presentation

Deutscher Orientalisten Tag 2022: [https://dot2022.de/wp-content/uploads/2022/06/DOT-Preliminary-Programme.pdf](https://dot2022.de/wp-content/uploads/2022/06/DOT-Preliminary-Programme.pdf)

### Spacial and Temporal Coverage in Šams ad-Dīn Muḥammad as-Saḫāwī's *Aḍ-Ḍawʾ al-lāmiʿ li-ahl al-qarn at-tāsiʿ* Analysed by Extraction and Enrichment of Historical Data

Biographical dictionaries are an essential part of the Arabic written tradition and a keystone of the Islamicate historiography.
With the digital turn in humanities there are now new ways of doing historical scholarship.
This project developed a digital humanties approach for automated extraction and enrichment of historical data from Šams ad-Dīn Muḥammad as-Saḫāwī’s (d. AH 902/AD 1497) *Aḍ-Ḍawʾ al-lāmiʿ li-ahl al-qarn at-tāsiʿ*.
The aim of this project is to apply digital humanities methods and show which potential for new insights these methods hold.
As part of the approach, biographies were automatically annotated using Named Entity Recognition, regular expressions and Natural Language Processing.
This data was in some cases enriched with further information to enable analysis.
The project also looked for sources suited for enrichment, such as existing gazetteers and using Wikidata to build new gazetteers.
The data obtained was then analysed und visualised.
In this presentation, the results of the geospacial coverage determined with the use of  Geographic Information Systems and the period covered will be presented.
Furthermore, the methods of computational analysis will be discussed and evaluated, which ones led to results and which ones turned out to be dead ends.

Keywords: Biographical Dictionaries; Historiography; Named Entity Recognition; Geographic Information Systems
