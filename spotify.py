import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import config
import pandas as pd
import json

client_id = config.CLIENT_ID
client_secret = config.CLIENT_SECRET

# Configuração do cliente Spotify
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)



# musicas = spotify.playlist_tracks(playlist_id="37i9dQZF1DWZjnMMy0dvW6")
# musicas_lista = musicas["items"]

# breakpoint()

# PIPELINE
def categorias(spotify):
    categorias = spotify.categories(country="BR", limit=20)
    categoria_list = categorias["categories"]["items"]

    categoria_id_lista = [categoria["id"] for categoria in categoria_list]
    categoria_nome_lista = [categoria["name"] for categoria in categoria_list]

    categoria_df = pd.DataFrame(list(zip(categoria_id_lista, categoria_nome_lista)), columns=["categoria_id", "categoria_nome"])
    categoria_df.to_csv("categorias.csv", index=False)

categorias(spotify)

# breakpoint()

def categoria_playlists(spotify):
    categoria_df = pd.read_csv("categorias.csv")
    categoria_id_lista = []
    playlist_id_lista = []
    playlist_descricao_lista = []
    playlist_nome_lista = []
    playlist_numero_musicas = []
    playlist_uri_lista = []

    for categoria_id in categoria_df["categoria_id"]:
        categoria_playlist_lista = spotify.category_playlists(category_id=categoria_id, country="BR", limit=20)
        categoria_playlist = categoria_playlist_lista["playlists"]["items"]

        for playlist in categoria_playlist:
            categoria_id_lista.append(categoria_id)
            playlist_id_lista.append(playlist["id"])
            playlist_descricao_lista.append(playlist["description"])
            playlist_nome_lista.append(playlist["name"])
            if playlist["tracks"]["total"] == None:
                playlist_numero_musicas.append(0)
            else:
                playlist_numero_musicas.append(playlist["tracks"]["total"])
            playlist_uri_lista.append(playlist["uri"])

    playlist_df = pd.DataFrame({
        "categoria_id": categoria_id_lista,
        "descricao": playlist_descricao_lista,
        "nome": playlist_nome_lista,
        "nr_musicas": playlist_numero_musicas,
        "playlist_id": playlist_id_lista,
        "uri": playlist_uri_lista,
    }
    )

    playlist_df.to_csv("playlist.csv", index=False)

categoria_playlists(spotify)

# breakpoint()

def musicas_albuns_artistas(spotify):
    playlist_df = pd.read_csv("playlist.csv")

    musicas_df = pd.DataFrame()
    albuns_df = pd.DataFrame()
    artistas_df = pd.DataFrame()

    for playlist in playlist_df["playlist_id"].head(20):
        print(f"Executing for Playlist - {playlist}")
        musicas = spotify.playlist_tracks(playlist_id=playlist)
        # se o musicas["items"] for vazio, pula para o proximo playlist
        if not musicas["items"]:
            continue
        musicas_lista = musicas["items"]

        # breakpoint()

        playlist_id_lista = []
        musicas_id_lista = []
        musicas_nome_lista = []
        musicas_duracao_lista = []
        musicas_popularidade_lista = []
        musicas_data_add_lista = []
        musicas_album_lista = []
        musicas_artista_lista = []
        musicas_uri_lista = []

        album_id_lista = []
        album_nome_lista = []
        album_tipo_lista = []
        album_data_lancamento_lista = []
        album_uri_lista = []
        album_artistas_nome_lista = []
        album_nr_musicas = []

        artista_id_lista = []
        artista_nome_lista = []
        artista_popularidade_lista = []
        artista_uri_lista = []
        artistas_seguidores_lista = []
        artistas_genero_lista = []

        for musica in musicas_lista:
            if musica["track"]:
                playlist_id_lista.append(playlist)
                musicas_data_add_lista.append(musica["added_at"])
                detalhes_musicas = musica["track"]
                musicas_duracao_lista.append(detalhes_musicas["duration_ms"])
                musicas_id_lista.append(detalhes_musicas["id"])
                musicas_nome_lista.append(detalhes_musicas["name"])
                musicas_popularidade_lista.append(detalhes_musicas["popularity"])
                musicas_uri_lista.append(detalhes_musicas["uri"])
                musicas_album_lista.append(detalhes_musicas["album"]["id"])

                detalhes_album = detalhes_musicas["album"]
                album_id_lista.append(detalhes_album["id"])
                album_nome_lista.append(detalhes_album["name"])
                album_tipo_lista.append(detalhes_album["album_type"])
                album_data_lancamento_lista.append(detalhes_album["release_date"])
                album_uri_lista.append(detalhes_album["uri"])
                album_nr_musicas.append(detalhes_album["total_tracks"])

                artista_lista = []
                detalhes_artistas = detalhes_musicas["artists"]

                for artista in detalhes_artistas:
                    artista_id_lista.append(artista["id"])
                    artista_lista.append(artista["id"])
                    artista_nome_lista.append(artista["name"])
                    artista_uri_lista.append(artista["uri"])

                    detalhe_artista = spotify.artist(artista["id"])
                    artistas_seguidores_lista.append(detalhe_artista["followers"]["total"])
                    artistas_genero_lista.append(detalhe_artista["genres"])
                    artista_popularidade_lista.append(detalhe_artista["popularity"])

                album_artistas_nome_lista.append(artista_lista)
                musicas_artista_lista.append(artista_lista)

        musicas_df_temp = pd.DataFrame({
            "playlist_id": playlist_id_lista,
            "data_add": musicas_data_add_lista,
            "duracao": musicas_duracao_lista,
            "musica_id": musicas_id_lista,
            "musica_nome": musicas_nome_lista,
            "popularidade": musicas_popularidade_lista,
            "uri": musicas_uri_lista,
            "album_id": musicas_album_lista,
            "artista_id": musicas_artista_lista,
        }
        )
        musicas_df = pd.concat([musicas_df, musicas_df_temp])

        album_df_temp = pd.DataFrame({
            "album_id": album_id_lista,
            "album_nome": album_nome_lista,
            "album_tipo": album_tipo_lista,
            "data_lancamento": album_data_lancamento_lista,
            "uri": album_uri_lista,
            "artistas_nome": album_artistas_nome_lista,
            "nr_musicas": album_nr_musicas,
        }
        )
        albuns_df = pd.concat([albuns_df, album_df_temp])

        artista_df_temp = pd.DataFrame({
            "artista_id": artista_id_lista,
            "artista_nome": artista_nome_lista,
            "popularidade": artista_popularidade_lista,
            "uri": artista_uri_lista,
            "seguidores": artistas_seguidores_lista,
            "genero": artistas_genero_lista,
        }
        )
        artistas_df = pd.concat([artistas_df, artista_df_temp])

    musicas_df.to_csv("musicas.csv", index=False)
    albuns_df.to_csv("albuns.csv", index=False)
    artistas_df.to_csv("artistas.csv", index=False)

musicas_albuns_artistas(spotify)
