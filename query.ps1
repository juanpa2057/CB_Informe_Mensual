# Activo mi entorno base y mi env
#& C:/ProgramData/Anaconda3/shell/condabin/conda-hook.ps1; conda activate base
& /opt/anaconda3/shell/condabin/conda-hook.ps1; conda activate base
& /opt/anaconda3/shell/condabin/conda-hook.ps1; conda activate myenv
# 2. Correr tools/request.ipynb para hacer el request a Ubidots.

#No se hace el request debido a que la data ya se trae del informe semanal
# 4. Borrar los notebooks individuales por sede (main/notebooks/individual)
& Remove-Item "main/notebooks/individual/*" -Recurse -Force


#5. Replicar el modelo para cada sede usando tools/builder.ipynb
& python tools/builder.py


#7. Borrar la carpeta main/\_build (o manipular la configuración de ejecución entre cache y force. Borrar funciona como hacer force)#
& Remove-Item  'main/_build' -Recurse -Force

# 10. Correr el comando: "jb build main" para compilar los notebooks
& jb build "main"

