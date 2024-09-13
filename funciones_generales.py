import pytz
import datetime
import os
import shutil
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


def fecha_peru_hoy():
    lima_timezone = pytz.timezone('America/Lima')
    lima_time = datetime.datetime.now(lima_timezone)
    return lima_time.date()

def restar_ano_en_string(fecha_string):
    fecha_string = fecha_string.strip("()")
    fecha_string = fecha_string.replace("'", "")

    fechas = fecha_string.split(',')

    fechas_modificadas = []

    for fecha in fechas:
        fecha = fecha.strip()
        
        year, campana = fecha.split('-')
        
        new_year = int(year) - 1
        
        nueva_fecha = f'{new_year}-{campana}'
        
        fechas_modificadas.append(nueva_fecha)
    
    return "(" + ", ".join("'" + f + "'" for f in fechas_modificadas) + ")"

def restar_ano_en_lista(lista_fechas):
    fechas_modificadas = []
    for item in lista_fechas:
        temp = []
        for fecha in item:
            fecha = fecha.strip()
            year, campana = fecha.split('-')
            new_year = int(year) - 1
            nueva_fecha = f'{new_year}-{campana}'
            temp.append(nueva_fecha)

        fechas_modificadas.append(temp)
    return fechas_modificadas

def upload_to_drive(file_path, folder_id):
    
    if not os.path.exists(file_path):
        print(f"El archivo {file_path} no existe.")
        return
    
    # Verificar si el archivo está vacío
    if os.path.getsize(file_path) == 0:
        print(f"El archivo {file_path} está vacío.")
        return
    # Autenticación y creación del cliente de Google Drive
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile("mycreds.txt")

    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()

    # Guardar las credenciales para la próxima vez
    gauth.SaveCredentialsFile("mycreds.txt")
    
    drive = GoogleDrive(gauth)

   # Crear y cargar el archivo en la carpeta especificada
    file_name = os.path.basename(file_path)
    file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
    file.SetContentFile(file_path)
    try:
        file.Upload()
        print(f"Archivo '{file_name}' subido exitosamente a Google Drive.")
    
    except Exception as e:
        print(f"Error al subir archivo: {e}")
    


    

    
def borrar_carpeta(folder):
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
        print("Deleting old reports...")