# Activo mi entorno base y mi env
& C:\ProgramData\Anaconda3\shell\condabin\conda-hook.ps1; conda activate base
& C:\ProgramData\Anaconda3\shell\condabin\conda-hook.ps1; conda activate nuevo2-env
#No se hace el request debido a que la data ya se trae del informe semanal
# 4. Borrar los notebooks individuales por sede (main/notebooks/individual)
& Remove-Item 'C:\Proyectos Digitalización\Bancolombia\CB_Informe_Mensual\main\notebooks\individual\*' -Recurse -Force

#5. Replicar el modelo para cada sede usando tools/builder.ipynb
& python tools\builder.py


#7. Borrar la carpeta main/\_build (o manipular la configuración de ejecución entre cache y force. Borrar funciona como hacer force)#
& Remove-Item 'main\_build' -Recurse
# 10. Correr el comando: "jb build main" para compilar los notebooks
& jb build "main"

