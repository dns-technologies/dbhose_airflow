DBHose-Airflow
==============

.. image:: /_static/title.png
   :alt: DBHose-Airflow Logo
   :align: center
   :width: 100%

**Граф зависимости модулей**

.. image:: /_static/graph.svg
   :alt: DBHose-Airflow Architecture
   :align: center
   :width: 100%
   :class: architecture-diagram

.. raw:: html

   <script>
   (function() {
       // Функция обновления изображения
       function updateGraph() {
           const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
           // Ищем все изображения с классом architecture-diagram
           const graphImgs = document.querySelectorAll('img.architecture-diagram');
           graphImgs.forEach(function(img) {
               // Проверяем что это наше изображение (содержит graph в src)
               if (img.src.includes('graph.svg') || img.src.includes('graph-dark.svg')) {
                   img.src = isDark ? '/_static/graph-dark.svg' : '/_static/graph.svg';
               }
           });
       }

       // Ждем загрузки страницы
       if (document.readyState === 'loading') {
           document.addEventListener('DOMContentLoaded', updateGraph);
       } else {
           // Небольшая задержка чтобы убедиться что все загрузилось
           setTimeout(updateGraph, 100);
       }
       
       // Следим за сменой темы
       const observer = new MutationObserver(function(mutations) {
           mutations.forEach(function(mutation) {
               if (mutation.attributeName === 'data-theme') {
                   updateGraph();
               }
           });
       });
       
       observer.observe(document.documentElement, { attributes: true });
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
