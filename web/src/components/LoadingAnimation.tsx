import React, { useEffect, useRef } from 'react';
import type {
    WebGLRenderer,
    LoadingManager
} from 'three';

interface LoadingAnimationProps {
    currentStage?: string;
    progress?: number;
}

const LoadingAnimation: React.FC<LoadingAnimationProps> = ({
                                                               currentStage = "Initializing...",
                                                               progress = 0
                                                           }) => {
    const containerRef = useRef<HTMLDivElement>(null);
    const rendererRef = useRef<WebGLRenderer | null>(null);
    const animationFrameRef = useRef<number | null>(null);
    const managerRef = useRef<LoadingManager | null>(null);

    useEffect(() => {
        console.log(`LoadingAnimation: Stage=${currentStage}, Progress=${progress}%`);
    }, [currentStage, progress]);

    useEffect(() => {
        const initThree = async () => {
            try {
                const THREE = await import('three');
                if (!containerRef.current) return;

                // Create LoadingManager
                const manager = new THREE.LoadingManager();
                managerRef.current = manager;

                manager.onProgress = (url, itemsLoaded, itemsTotal) => {
                    const progressPercentage = (itemsLoaded / itemsTotal) * 100;
                    console.log(`Loading progress: ${progressPercentage}%`);
                };

                // Scene setup
                const scene = new THREE.Scene();
                const camera = new THREE.PerspectiveCamera( 75, window.innerWidth / window.innerHeight, 0.1, 1000 );
                const renderer = new THREE.WebGLRenderer();
                renderer.setSize(containerRef.current.clientWidth, containerRef.current.clientHeight);
                containerRef.current.appendChild(renderer.domElement);

                renderer.setAnimationLoop( animate );

                const geometry = new THREE.TorusKnotGeometry(10, 3, 100, 16);
                const material = new THREE.MeshNormalMaterial({});
                const torusKnot = new THREE.Mesh(geometry, material);
                scene.add(torusKnot);

                camera.position.z = 50;

                torusKnot.rotation.x = Math.PI / 4;
                torusKnot.rotation.y = Math.PI / 6;

                function animate() {
                    torusKnot.rotation.x += 0.01;
                    torusKnot.rotation.y += 0.01;
                    renderer.render( scene, camera );
                }

                animate();

                // Handle window resize
                const handleResize = () => {
                    if (!camera || !renderer || !containerRef.current) return;

                    camera.aspect = containerRef.current.clientWidth / containerRef.current.clientHeight;
                    camera.updateProjectionMatrix();
                    renderer.setSize(containerRef.current.clientWidth, containerRef.current.clientHeight);
                };

                window.addEventListener('resize', handleResize);

                return () => {
                    window.removeEventListener('resize', handleResize);
                    if (animationFrameRef.current) {
                        cancelAnimationFrame(animationFrameRef.current);
                    }
                    if (containerRef.current && rendererRef.current) {
                        containerRef.current.removeChild(rendererRef.current.domElement);
                    }
                    renderer.dispose();
                };
            } catch (err) {
                console.error('Error setting up Three.js:', err);
            }
        };

        initThree();
    }, []);

    return (
        <div className="fixed inset-0 flex flex-col items-center justify-center bg-black z-50">
            <div ref={containerRef} className="w-full h-full" />
            <p className="absolute bottom-16 text-xl text-white">
                {currentStage} ({Math.round(progress)}%)
            </p>
        </div>
    );
};

export default LoadingAnimation;