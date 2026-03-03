DBHose-Airflow
==============

.. image:: /_static/title.png
   :alt: DBHose-Airflow Logo
   :align: center
   :width: 100%

**Граф зависимости модулей**

.. only:: html

   .. image:: /_static/graph.svg
      :alt: DBHose-Airflow Architecture
      :align: center
      :width: 100%
      :class: architecture-diagram

.. only:: latex

   .. image:: /_static/graph.svg
      :alt: DBHose-Airflow Architecture
      :align: center
      :width: 100%
      :class: architecture-diagram

.. raw:: html

   <script>
   document.addEventListener('DOMContentLoaded', function() {
       function updateGraph() {
           const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
           const graphImg = document.querySelector('.architecture-diagram img');
           if (graphImg) {
               graphImg.src = isDark ? '/_static/graph-dark.svg' : '/_static/graph.svg';
           }
       }
       
       updateGraph();
       
       const observer = new MutationObserver(updateGraph);
       observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
   });
   </script>

.. toctree::
   :maxdepth: 2
   :caption: Содержание:

   overview/index
   quickstart/index
   classes/index
   functions/index
   enums/index
   constants/index
   base_modules/index

.. toctree::
   :hidden:
   :caption: Линки на проект:

   GitHub <https://github.com/dns-technologies/dbhose_airflow>
   PyPI <https://pypi.org/project/dbhose-airflow/>
