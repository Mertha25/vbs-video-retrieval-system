// src/data/mockVideos.ts
import { Video } from '../components/VideoCard/VideoCard';

// (remove or comment out the local interface Video)

export const mockVideos: Video[] = [
  {
    id: '1',
    title: 'Big Buck Bunny (240p, 10s)',
    video_path: 'https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/240/Big_Buck_Bunny_240_10s_1MB.mp4',
    duration: 10,
    score: 0.97,
    timestamp: 2.3,
    objects: ['bunny', 'butterfly'],
    text: [],
    dominant_colors: [
      [162, 212, 248],
      [124, 168, 92],
      [236, 196, 112],
    ],
  },
  {
    id: '2',
    title: 'Sintel Trailer (240p, 10s)',
    video_path: 'https://test-videos.co.uk/vids/sintel/mp4/h264/240/Sintel_240_10s_1MB.mp4',
    duration: 10,
    score: 0.91,
    timestamp: 5.1,
    objects: ['dragon', 'girl'],
    text: ['Sintel'],
    dominant_colors: [
      [42, 38, 34],
      [176, 108, 60],
      [208, 184, 160],
    ],
  },
  {
    id: '3',
    title: 'Tears of Steel (240p, 10s)',
    video_path: 'https://test-videos.co.uk/vids/tears_of_steel/mp4/h264/240/Tears_of_Steel_240_10s_1MB.mp4',
    duration: 10,
    score: 0.89,
    timestamp: 6.8,
    objects: ['robot', 'street'],
    text: [],
    dominant_colors: [
      [56, 64, 78],
      [104, 112, 128],
      [200, 152, 96],
    ],
  },
  {
    id: '4',
    title: 'Big Buck Bunny (480p, 10s)',
    video_path: 'https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/480/Big_Buck_Bunny_480_10s_2MB.mp4',
    duration: 10,
    score: 0.95,
    timestamp: 1.7,
    objects: ['bunny', 'tree'],
    text: [],
    dominant_colors: [
      [144, 200, 80],
      [236, 212, 148],
      [92, 136, 60],
    ],
  },
  {
    id: '5',
    title: 'Sintel Trailer (480p, 10s)',
    video_path: 'https://test-videos.co.uk/vids/sintel/mp4/h264/480/Sintel_480_10s_2MB.mp4',
    duration: 10,
    score: 0.93,
    timestamp: 4.4,
    objects: ['girl', 'snow'],
    text: ['Trailer'],
    dominant_colors: [
      [80, 68, 60],
      [200, 132, 88],
      [224, 200, 176],
    ],
  },
  {
    id: '6',
    title: 'Tears of Steel (480p, 10s)',
    video_path: 'https://test-videos.co.uk/vids/tears_of_steel/mp4/h264/480/Tears_of_Steel_480_10s_2MB.mp4',
    duration: 10,
    score: 0.88,
    timestamp: 7.2,
    objects: ['building', 'laser'],
    text: [],
    dominant_colors: [
      [72, 80, 96],
      [112, 120, 136],
      [192, 152, 108],
    ],
  },
];
