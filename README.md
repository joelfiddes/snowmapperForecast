# SnowMapper
**SnowMapper** is a tool that generates high-resolution maps of snow variables (snow height, snow water equivalent and snow melt) at daily timescales. The SnowMapper is an online operational snow mapping and forecasting tool for High-Mountain Central Asia (HMCA). The SnowMapper is available at https://snowmapper.ch/. The two main components required to achieve this are the climate downscaling tool **TopoPyscale** (https://topopyscale.readthedocs.io/) and the Factorial Snow Model, **FSM**, developed by Richard Essery (Essery 2015, https://github.com/RichardEssery/FSM). Visualisation and web deployment is handled by the MCASS package (https://github.com/hydrosolutions/MCASS). SnowMapper was developed as a collaboration between the WSL Institute for Snow and Avalanche Research SLF and hydrosolutions GmbH within the SDC-funded projects CROMO-ADAPT and SAPPHIRE.

<div align="center">
    <img width="700" alt="Figure 1" src="https://github.com/user-attachments/assets/b67a220e-9947-4bc7-9ca5-dff83241fae9">
</div>




## Methods
SnowMapper relies on several modules in its model pipeline as shown in Figure 2:

<div align="center">
<img width="701" alt="Figure 2" src="https://github.com/user-attachments/assets/9f6b8f2c-219b-450b-8908-69af3335d58d">
<div style="font-style: italic; margin-top: 8px;"><em>   Figure 2: The SnowMapper model pipeline.  </em></div>
</div>

### Downscaling: TopoPyscale 
TopoPyscale converts low-resolution climate and weather data—available as both historical datasets and forecasts—into high-resolution datasets that account for the variability of meteorological variables in mountainous regions. A simple example of this phenomenon is the strong temperature gradients that occur over short distances as one moves from valley bottoms to mountain peaks. Global climate and weather products are available operationally from several global providers, such as the European Center for Medium-Range Forecasts (ECMWF), allowing services like SnowMapper to run autonomously. Downscaled climate and weather data together with the FSM snow model, then allow the production of high-resolution snow maps. 

<div align="center">
<img width="364" alt="Figure 3" src="https://github.com/user-attachments/assets/b160802c-b6b8-4d3b-8782-c60cd9916b4f">
        <div style="font-style: italic; margin-top: 8px;"><em>Figure 3: Comparison of the near-surface air temperature on January 1, 2020 at 12:00 UTC over the Southern Carpathians, Romania, in between ERA5 Surface product and the downscaled result using TopoPyScale (Filhol et al. 2023, Fiddes et al. 2014).  </em></div>
</div>

### Snow model: FSM 
FSM is a versatile coupled mass and energy balance snow model often used in cold region hydrology. The FSM is structured to allow flexible adjustments in the complexity of the snow processes it simulates, accommodating a range of configurations from very simple to relatively detailed physical representations. The model includes modular components to represent key snow processes such as: 
- The model includes modular components to represent key snow processes like: 
- Snow accumulation: capturing snowfall and snowpack formation. 
- Melting and energy balance: using different approaches, from simple degree-day models to energy-balance approaches. 
- Albedo decay: tracking how snow reflectivity changes over time. 
- Snow density and compaction: representing changes in snow density due to settling and compaction over time.

### Mapping results
To run numerical models at high resolution over very large domains some form of simplification is usually needed. In this case we use the tool TopoSUB that generates topographical samples analogous to HRUs in hydrology. However, here samples are discontinuous in space and only defined by the mean attributes of the predictors: elevation, slope, aspect, sky view factor, longitude and latitude. In this way the main dimensions of variability in the model domain are represented, while reducing computational loads by several orders of magnitude (see Fiddes & Gruber 2012 for full details). The input is a digital elevation model (and derived parameters) that defines the model domain and resolution. A K-means clustering algorithm is employed to generate samples from input predictors and as a result all pixels are mapped to an output sample. One-dimensional model simulations are run on each TopoSUB sample and then results are remapped back to pixels (many) that belong to the sample (few).

<div align="center">
<img width="500" alt="Figure 4" src="https://github.com/user-attachments/assets/ccb4db43-0b65-49c2-8ef9-eab2ed30fb26">
    <div style="font-style: italic; margin-top: 8px;"><em>Figure: A simplified example of multidimensional sampling used by TopoSUB. The heterogeneous subgrid is grouped into samples S1-5 according to three dimensions P1-3 which define the parameter space (3-D cube). Colour represents sample membership, shade of colour symbolises within sample variation with respect to P1-3. This method generalises to higher sample numbers, more highly resolved subgrids and higher orders of dimensionality. </em></div>
</div>

<div style="text-align: center;">
    <img width="906" alt="Figure 4" src="https://github.com/user-attachments/assets/3a1969c5-d30b-452d-a408-f38423e64544">
    <div style="font-style: italic; margin-top: 8px;"><em>Figure: Sample SWE-situation in high-mountain Central Asia on 29. November 2011. SWE is computed using the FSM multi-physics snow model. The SnowMapper covers more than 400’000 km2 and encompasses 300 gauged basins in Kazakhstan, Kyrgyzstan, Tajikistan, Uzbekistan, and Afghanistan (figure produced by hydrosolutions GmbH). </em></div>
</div>



## Data
SnowMapper is driven by two ECMWF data sources which are both output of the IFS NWP model:

(1) ECMWF ERA5 / ERA5T reanalysis https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5
(2) ECMWF open forecast https://www.ecmwf.int/en/forecasts/datasets/open-data

Reanalysis provides data with a latency of 5 days. 10 day forecasts are issued at a varying timestep (3-6h) twice per day with start at 00 UTC and 12 UTC. Release is c. 7h after start timestep.

Minor differences between the datasets in available variables are handled internally by SnowMapper, ensuring they are passed to TopoPyscale as a consistent, seamless time series that spans a moving window from T-30 days to T+10 days, where T is the current day.




## Operational deployment

## References

