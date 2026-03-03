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
      :id: theme-graph
      :class: architecture-diagram

.. only:: latex

   .. image:: /_static/graph.svg
      :alt: DBHose-Airflow Architecture
      :align: center
      :width: 100%
      :class: architecture-diagram

.. raw:: html

   <script>
   (function() {
       function updateGraph() {
           const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
           const graphImg = document.getElementById('theme-graph');
           if (graphImg) {
               graphImg.src = isDark ? '/_static/graph-dark.svg' : '/_static/graph.svg';
           }
       }
       
       // Ждем полной загрузки DOM
       if (document.readyState === 'loading') {
           document.addEventListener('DOMContentLoaded', updateGraph);
       } else {
           updateGraph();
       }
       
       // Отслеживаем изменение темы
       const observer = new MutationObserver(updateGraph);
       observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
   })();
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
