from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ContentExtractor:
    """Extracts text, images, and videos from Bluesky posts."""

    # Define text field paths to extract
    TEXT_PATHS = [
        "message.commit.record.text",
        "message.commit.record.embed.external.description",
        "message.commit.record.embed.external.title",
        "message.commit.record.embed.external.thumb.alt",
        # Note: Removed "message.commit.record.embed.images[].alt" to avoid concatenation
        # Alt texts are now handled individually with each image
        "message.commit.record.embed.record.value.text",
        "message.commit.record.embed.video.alt",
        "message.hydrated_metadata.user.description",
        "message.hydrated_metadata.parent_post.record.text",
        "message.hydrated_metadata.parent_post.record.embed.external.description",
        "message.hydrated_metadata.parent_post.record.embed.external.title",
        # Removed array notation for alt texts
        "message.hydrated_metadata.reply_post.record.text",
        "message.hydrated_metadata.quote_post.record.text",
        "message.hydrated_metadata.quote_post.record.embed.video.alt",
        "message.hydrated_metadata.quote_post.value.text",
        "message.hydrated_metadata.quote_post.embed.record.value.text"
    ]

    def extract_text_fields(self, post: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract all text fields from a post.
        
        Returns:
            Dict mapping field path to text content
        """
        texts = {}

        for path in self.TEXT_PATHS:
            try:
                value = self._extract_by_path(post, path)
                if value:
                    texts[path] = value
            except Exception as e:
                logger.debug(f"Error extracting {path}: {e}")

        return texts

    def extract_images(self, post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all images from a post.
        
        Returns:
            List of dicts with 'url' and 'data' keys
        """
        images = []
        did = post.get("did")

        # Direct image embed
        if self._get_nested(post, "message.commit.record.embed.$type") == "app.bsky.embed.images":
            embed_images = self._get_nested(post, "message.commit.record.embed.images") or []
            images.extend(self._process_image_list(embed_images, did))

        # External embed with thumbnail
        if (self._get_nested(post, "message.commit.record.embed.$type") == "app.bsky.embed.external" and
            self._get_nested(post, "message.commit.record.embed.external.thumb")):
            thumb = self._get_nested(post, "message.commit.record.embed.external.thumb")
            if thumb:
                processed = self._process_image(thumb, is_thumb=True, did=did)
                if processed:
                    images.append(processed)

        # Record with media (images) - IMPORTANT: handle galleries
        if self._get_nested(post, "message.commit.record.embed.$type") == "app.bsky.embed.recordWithMedia":
            # Check the media type
            media = self._get_nested(post, "message.commit.record.embed.media")
            if media and media.get("$type") == "app.bsky.embed.images":
                media_images = media.get("images") or []
                images.extend(self._process_image_list(media_images, did))

        # Images in hydrated metadata
        for path in [
            "message.hydrated_metadata.parent_post.embed.images",
            "message.hydrated_metadata.parent_post.embed.media.images",
            "message.hydrated_metadata.quote_post.embed.images",
            "message.hydrated_metadata.quote_post.embed.media.images",
            "message.hydrated_metadata.parent_post.record.embed.images",
            "message.hydrated_metadata.reply_post.embed.images",
            "message.hydrated_metadata.reply_post.embed.media.images"
        ]:
            embed_images = self._get_nested(post, path)
            if embed_images:
                # Try to get DID from the parent/quote/reply post
                parent_did = None
                if "parent_post" in path:
                    parent_did = self._get_nested(post, "message.hydrated_metadata.parent_post.did")
                elif "quote_post" in path:
                    parent_did = self._get_nested(post, "message.hydrated_metadata.quote_post.did")
                elif "reply_post" in path:
                    parent_did = self._get_nested(post, "message.hydrated_metadata.reply_post.did")
                images.extend(self._process_image_list(embed_images, parent_did or did))

        return images

    def extract_videos(self, post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all videos from a post.
        
        Returns:
            List of dicts with 'url', 'data', and 'needs_did_resolution' keys
        """
        videos = []
        did = post.get("did")
        
        # Direct video embed
        if self._get_nested(post, "message.commit.record.embed.$type") == "app.bsky.embed.video":
            video = self._get_nested(post, "message.commit.record.embed.video")
            if video:
                processed = self._process_video(video, did)
                if processed:
                    videos.append(processed)

        # Record with media (video)
        if (self._get_nested(post, "message.commit.record.embed.$type") == "app.bsky.embed.recordWithMedia" and
            self._get_nested(post, "message.commit.record.embed.media.$type") == "app.bsky.embed.video"):
            video = self._get_nested(post, "message.commit.record.embed.media.video")
            if video:
                processed = self._process_video(video, did)
                if processed:
                    videos.append(processed)

        # External media that might be video
        if (self._get_nested(post, "message.commit.record.embed.$type") == "app.bsky.embed.recordWithMedia" and
            self._get_nested(post, "message.commit.record.embed.media.$type") == "app.bsky.embed.external"):
            external = self._get_nested(post, "message.commit.record.embed.media.external")
            if external:
                uri = external.get("uri", "")
                # Check for common video extensions
                video_extensions = ['.mp4', '.webm', '.avi', '.mov', '.mkv', '.m4v']
                if any(uri.lower().endswith(ext) for ext in video_extensions):
                    videos.append({
                        "url": uri,
                        "data": None,
                        "alt": external.get("alt", ""),
                        "needs_did_resolution": False
                    })

        # Videos in hydrated metadata
        for path in [
            "message.hydrated_metadata.parent_post.embed.video",
            "message.hydrated_metadata.quote_post.embed.video"
        ]:
            video = self._get_nested(post, path)
            if video:
                # Try to get DID from the parent/quote post
                parent_did = None
                if "parent_post" in path:
                    parent_did = self._get_nested(post, "message.hydrated_metadata.parent_post.did")
                elif "quote_post" in path:
                    parent_did = self._get_nested(post, "message.hydrated_metadata.quote_post.did")

                processed = self._process_video(video, parent_did or did)
                if processed:
                    videos.append(processed)

        return videos

    def extract_embedding_text(self, post: Dict[str, Any]) -> str:
        """
        Extract the text field used for text embeddings from a post.
        
        Returns:
            Dict mapping field path to text content
        """

        return post.get("message", {}).get("commit", {}).get("record", {}).get("text", "")

    def _extract_by_path(self, data: Dict[str, Any], path: str) -> Optional[str]:
        """Extract value by dot-separated path, handling arrays."""
        if "[]" in path:
            # Handle array notation
            parts = path.split("[]")
            base_path = parts[0]
            remaining_path = parts[1].lstrip(".") if len(parts) > 1 else None

            array_value = self._get_nested(data, base_path)
            if not isinstance(array_value, list):
                return None

            # Collect values from array
            values = []
            for item in array_value:
                if remaining_path:
                    val = self._get_nested(item, remaining_path)
                else:
                    val = item

                if val and isinstance(val, str):
                    values.append(val)

            return " ".join(values) if values else None
        else:
            # Simple path
            value = self._get_nested(data, path)
            return value if isinstance(value, str) else None

    def _get_nested(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested value from dict using dot notation."""
        current = data
        for part in path.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        return current

    def _process_image_list(self, images: List[Dict[str, Any]], did: str | None = None) -> List[Dict[str, Any]]:
        """Process a list of image objects."""
        processed = []
        for img in images:
            result = self._process_image(img, did=did)
            if result:
                processed.append(result)
        return processed

    def _process_image(self, image: Dict[str, Any], is_thumb: bool = False, did: str | None = None) -> Optional[Dict[str, Any]]:
        """Process a single image object."""
        if is_thumb:
            # External thumbnail
            url = image.get("uri") or image.get("url")
            if url:
                return {
                    "url": url,
                    "data": None,  # Will be fetched if needed
                    "alt": image.get("alt", "")
                }
        else:
            # Regular image embed
            image_info = image.get("image") or {}
            cid = image_info.get("ref", {}).get("$link")

            if cid and did:
                # Construct CDN URL for Bluesky image
                url = f"https://cdn.bsky.app/img/feed_thumbnail/plain/{did}/{cid}@jpeg"
                return {
                    "url": url,
                    "data": None,  # Will be fetched if needed
                    "alt": image.get("alt", ""),
                    "aspect_ratio": image.get("aspectRatio")
                }
            else:
                # Fallback to other URL formats
                url = image_info.get("uri") or image_info.get("url")
                if url:
                    return {
                        "url": url,
                        "data": None,  # Will be fetched if needed
                        "alt": image.get("alt", ""),
                        "aspect_ratio": image.get("aspectRatio")
                    }

        return None

    def _process_video(self, video: Dict[str, Any], did: str | None = None) -> Optional[Dict[str, Any]]:
        """Process a video object."""
        video_info = video.get("video") or video
        cid = video_info.get("ref", {}).get("$link")

        if cid and did:
            # Video needs DID resolution to get PDS endpoint
            return {
                "url": None,  # Will be constructed after DID resolution
                "cid": cid,
                "did": did,
                "data": None,
                "alt": video.get("alt", ""),
                "aspect_ratio": video.get("aspectRatio"),
                "needs_did_resolution": True
            }
        else:
            # Try direct URL
            url = video_info.get("uri") or video_info.get("url")
            if url:
                return {
                    "url": url,
                    "data": None,
                    "alt": video.get("alt", ""),
                    "aspect_ratio": video.get("aspectRatio"),
                    "needs_did_resolution": False
                }

        return None
