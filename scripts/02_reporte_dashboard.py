import os
import sys
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

ruta_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "db", "musica_destino.db")
ruta_salida = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "output")
os.makedirs(ruta_salida, exist_ok=True)

if not os.path.exists(ruta_db):
    sys.exit(f"Error: No existe {ruta_db}. Ejecuta primero el DAG de Airflow.")

conn = sqlite3.connect(ruta_db)

color_grammy = '#2a78d6'
color_sin_grammy = '#eb6834'
colores = [color_grammy, color_sin_grammy]

print("Generando gráficas individuales...")

df_pop = pd.read_sql("""
    SELECT CASE WHEN has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END AS grupo,
           AVG(popularity) AS popularidad_promedio,
           COUNT(*) AS canciones
    FROM spotify_grammys GROUP BY grupo ORDER BY grupo
""", conn)

plt.figure(figsize=(8, 5))
plt.bar(df_pop['grupo'], df_pop['popularidad_promedio'], color=colores)
plt.title('1. Popularidad Promedio: con vs. sin Grammy')
plt.ylabel('Popularidad (0-100)')
plt.savefig(os.path.join(ruta_salida, '1_popularidad.png'), bbox_inches='tight')
plt.close()

df_top = pd.read_sql("""
    SELECT main_artist, MAX(grammy_wins) AS grammys
    FROM spotify_grammys WHERE has_grammy = 1
    GROUP BY artist_key ORDER BY grammys DESC LIMIT 10
""", conn)
df_top = df_top.iloc[::-1] # Invertir para que el mayor quede arriba

plt.figure(figsize=(8, 5))
plt.barh(df_top['main_artist'], df_top['grammys'], color=color_grammy)
plt.title('2. Top 10 Artistas con más Grammys en Spotify')
plt.xlabel('Grammys ganados')
plt.savefig(os.path.join(ruta_salida, '2_top_artistas.png'), bbox_inches='tight')
plt.close()

df_gen = pd.read_sql("""
    SELECT track_genre, ROUND(100.0 * SUM(has_grammy) / COUNT(*), 1) AS pct_con_grammy
    FROM spotify_grammys GROUP BY track_genre HAVING COUNT(*) >= 50
    ORDER BY pct_con_grammy DESC LIMIT 10
""", conn)
df_gen = df_gen.iloc[::-1]

plt.figure(figsize=(8, 5))
plt.barh(df_gen['track_genre'], df_gen['pct_con_grammy'], color=color_grammy)
plt.title('3. Géneros con mayor % de canciones de artistas con Grammy')
plt.xlabel('% de canciones')
plt.savefig(os.path.join(ruta_salida, '3_generos.png'), bbox_inches='tight')
plt.close()

df_audio = pd.read_sql("""
    SELECT CASE WHEN has_grammy = 1 THEN 'Con Grammy' ELSE 'Sin Grammy' END AS grupo,
           AVG(danceability) AS danceability, AVG(energy) AS energy,
           AVG(valence) AS valence, AVG(acousticness) AS acousticness,
           AVG(speechiness) AS speechiness
    FROM spotify_grammys GROUP BY grupo ORDER BY grupo
""", conn)

df_audio.set_index('grupo').T.plot(kind='bar', figsize=(10, 5), color=colores)
plt.title('4. Perfil de Audio Promedio')
plt.xticks(rotation=0)
plt.savefig(os.path.join(ruta_salida, '4_audio.png'), bbox_inches='tight')
plt.close()

df_dec = pd.read_sql("""
    SELECT (CAST(first_grammy_year AS INTEGER) / 10) * 10 AS decada,
           COUNT(DISTINCT artist_key) AS artistas
    FROM spotify_grammys WHERE has_grammy = 1 GROUP BY decada ORDER BY decada
""", conn)

plt.figure(figsize=(8, 5))
plt.bar(df_dec['decada'].astype(int).astype(str) + "s", df_dec['artistas'], color=color_grammy)
plt.title('5. Artistas con Grammy según década del primer premio')
plt.ylabel('Cantidad de artistas')
plt.savefig(os.path.join(ruta_salida, '5_decadas.png'), bbox_inches='tight')
plt.close()

plt.figure(figsize=(8, 5))
plt.bar(df_pop['grupo'], df_pop['canciones'], color=colores)
plt.title('6. Número de canciones según su artista')
plt.ylabel('Total de canciones')
plt.savefig(os.path.join(ruta_salida, '6_canciones.png'), bbox_inches='tight')
plt.close()


print("Generando dashboard combinado (subplots)...")
fig, axs = plt.subplots(3, 2, figsize=(16, 18))


axs[0, 0].bar(df_pop['grupo'], df_pop['popularidad_promedio'], color=colores)
axs[0, 0].set_title('1. Popularidad Promedio: con vs. sin Grammy')

axs[0, 1].barh(df_top['main_artist'], df_top['grammys'], color=color_grammy)
axs[0, 1].set_title('2. Top 10 Artistas con más Grammys en Spotify')

axs[1, 0].barh(df_gen['track_genre'], df_gen['pct_con_grammy'], color=color_grammy)
axs[1, 0].set_title('3. Géneros con mayor % (Artistas con Grammy)')

df_audio.set_index('grupo').T.plot(kind='bar', ax=axs[1, 1], color=colores)
axs[1, 1].set_title('4. Perfil de Audio Promedio (0-1)')
axs[1, 1].tick_params(axis='x', rotation=0)

axs[2, 0].bar(df_dec['decada'].astype(int).astype(str) + "s", df_dec['artistas'], color=color_grammy)
axs[2, 0].set_title('5. Artistas con Grammy según década del primer premio')

axs[2, 1].bar(df_pop['grupo'], df_pop['canciones'], color=colores)
axs[2, 1].set_title('6. Canciones según su artista principal')

plt.tight_layout()
plt.savefig(os.path.join(ruta_salida, 'dashboard_spotify_grammys.png'))
plt.close()

print("Gráficas individuales y el dashboard completo fueron guardados en la carpeta data/output/")
conn.close()