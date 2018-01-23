package de.ustutt.iaas.cc.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;

import de.ustutt.iaas.cc.api.Note;
import de.ustutt.iaas.cc.api.NoteWithText;

/**
 * DAO implementation that stores notes as entities in Google Datastore (NoSQL).
 * 
 * @author hauptfn
 *
 */
public class GoogleDatastoreNotebookDAO implements INotebookDAO {

	private final static Logger logger = LoggerFactory.getLogger(GoogleDatastoreNotebookDAO.class);
	private static final String ENTITY_KIND = "Note";
	private static final String PROP_AUTHOR = "author";
	private static final String PROP_TEXT = "text";

	private Datastore datastore;
	private KeyFactory keyFactory;

	public GoogleDatastoreNotebookDAO(String gcKeyFile, String gcProjectId) {
		super();
		try {
			GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(gcKeyFile));
			DatastoreOptions options = DatastoreOptions.newBuilder().setProjectId(gcProjectId)
					.setCredentials(credentials).build();
			this.datastore = options.getService();
			this.keyFactory = this.datastore.newKeyFactory().setKind(ENTITY_KIND);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Set<Note> getNotes() {
		Set<Note> result = new HashSet<Note>();
		Query<Entity> query = Query.newEntityQueryBuilder().setKind(ENTITY_KIND).build();
		QueryResults<Entity> queryResults = this.datastore.run(query);

		logger.info("getNotes:");
		while (queryResults.hasNext()) {
			Entity currentEntity = queryResults.next();
			logger.info("key/id: {}", currentEntity.getKey().getId());
			logger.info("author: {}", currentEntity.getString(PROP_AUTHOR));
			result.add(new Note(currentEntity.getKey().getId().toString(), currentEntity.getString(PROP_AUTHOR)));
		}
		return result;
	}

	@Override
	public NoteWithText getNote(String noteID) {
		Entity currentEntity = this.datastore.get(this.keyFactory.newKey(Long.valueOf(noteID)));
		NoteWithText result = new NoteWithText(currentEntity.getKey().getId().toString(),
				currentEntity.getString(PROP_AUTHOR), currentEntity.getString(PROP_TEXT));
		return result;
	}

	@Override
	public NoteWithText createOrUpdateNote(NoteWithText note) {
		NoteWithText result = null;
		if (note != null) {
			Key key = null;
			if (StringUtils.isBlank(note.getId())) {
				key = this.datastore.allocateId(this.keyFactory.newKey());
				note.setId(key.getId().toString());
			} else {
				key = this.keyFactory.newKey(Long.valueOf(note.getId()));
			}
			logger.info("createOrUpdate: key/id: {}", key.getId());
			Entity entity = Entity.newBuilder(key).set(PROP_AUTHOR, note.getAuthor()).set(PROP_TEXT, note.getText())
					.build();
			this.datastore.put(entity);
			result = note;
		}
		return result;
	}

	@Override
	public void deleteNote(String noteID) {
		this.datastore.delete(this.keyFactory.newKey(Long.valueOf(noteID)));
	}

}
