import psycopg2

conn = psycopg2.connect(host='postgres', port=5432, user='postgres', password='admin123', database='videodb_creative_v2')
cursor = conn.cursor()

cursor.execute("UPDATE videos SET compressed_filename = video_id || '.mp4' WHERE compressed_filename = 'compressed_for_web.mp4'")
print(f'Updated {cursor.rowcount} records')

conn.commit()
cursor.close()
conn.close() 