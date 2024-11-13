# SnowMapper
**SnowMapper** is a tool that generates high-resolution maps of snow variables (snow height, snow water equivalent and snow melt) at daily timescales. The SnowMapper is an online operational snow mapping and forecasting tool for High-Mountain Central Asia (HMCA). The SnowMapper is available at https://snowmapper.ch/. The two main components required to achieve this are the climate downscaling tool **TopoPyscale** (https://topopyscale.readthedocs.io/) and the Factorial Snow Model, **FSM**, developed by Richard Essery (Essery 2015, https://github.com/RichardEssery/FSM).

<div align="center">
    <img width="700" alt="image" src="https://github.com/user-attachments/assets/b67a220e-9947-4bc7-9ca5-dff83241fae9">
</div>


**TopoPyscale** converts low-resolution climate and weather data—available as both historical datasets and forecasts—into high-resolution datasets that account for the variability of meteorological variables in mountainous regions. A simple example of this phenomenon is the strong temperature gradients that occur over short distances as one moves from valley bottoms to mountain peaks. Global climate and weather products are available operationally from several global providers, such as the European Center for Medium-Range Forecasts (ECMWF), allowing services like SnowMapper to run autonomously. Downscaled climate and weather data together with the FSM snow model, then allow the production of high-resolution snow maps. 

**FSM** is a versatile coupled mass and energy balance snow model often used in cold region hydrology. The FSM is structured to allow flexible adjustments in the complexity of the snow processes it simulates, accommodating a range of configurations from very simple to relatively detailed physical representations. The model includes modular components to represent key snow processes such as: 
- The model includes modular components to represent key snow processes like: 
- Snow accumulation: capturing snowfall and snowpack formation. 
- Melting and energy balance: using different approaches, from simple degree-day models to energy-balance approaches. 
- Albedo decay: tracking how snow reflectivity changes over time. 
- Snow density and compaction: representing changes in snow density due to settling and compaction over time. 


